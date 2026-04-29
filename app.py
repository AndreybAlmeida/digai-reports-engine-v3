"""
DigAI Reports Engine — Interface Web

Local:    python3 app.py  →  http://localhost:5001
Produção: gunicorn app:app (Railway/Render configura automaticamente via PORT)
"""

from __future__ import annotations

import gc
import json
import os
import re
import shutil
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, request, render_template_string, redirect, jsonify, send_file, current_app, session

import sys
sys.path.insert(0, str(Path(__file__).parent))

# Carrega variáveis do .env se existir (sem dependência de python-dotenv)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())
from engine.pipeline import run as _pipeline_run, PipelineError
from engine.analytics import gerar_relatorio_from_sources, gerar_relatorio
from engine.dimensions import detect_dimensions
from engine.excel_gen import gerar_excel as _gerar_excel_novo

UPLOAD_DIR     = Path(__file__).parent / "uploads"
REPORTS_DIR    = Path(__file__).parent / "reports"

# DASHBOARDS_DIR: usa volume persistente se disponível (variável PERSISTENT_DIR no Render/Railway).
# Sem essa variável, usa pasta local — que é apagada em todo restart do container.
# Para Render: Settings → Disks → Mount Path: /data → set PERSISTENT_DIR=/data
_persistent_root = os.environ.get("PERSISTENT_DIR", "")
if _persistent_root and Path(_persistent_root).exists():
    DASHBOARDS_DIR = Path(_persistent_root) / "dashboards"
else:
    DASHBOARDS_DIR = Path(__file__).parent / "dashboards"

UPLOAD_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
DASHBOARDS_DIR.mkdir(exist_ok=True)

# Arquivo de registro de links gerados (sobrevive a restarts quando em volume persistente)
_REGISTRY_FILE = DASHBOARDS_DIR / "registry.json"
_REGISTRY_LOCK_FILE = str(_REGISTRY_FILE) + ".lock"

try:
    from filelock import FileLock as _FileLock
    _REGISTRY_LOCK = _FileLock(_REGISTRY_LOCK_FILE, timeout=10)
except ImportError:
    # Fallback sem lock se filelock não estiver instalado (nunca deve acontecer em produção)
    import contextlib
    _REGISTRY_LOCK = contextlib.nullcontext()


def _registry_load() -> list:
    """Carrega o registro de links gerados (chamado dentro do lock)."""
    try:
        if _REGISTRY_FILE.exists():
            return json.loads(_REGISTRY_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def _registry_save(entries: list):
    """Salva o registro de links (chamado dentro do lock)."""
    try:
        _REGISTRY_FILE.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"[registry] Erro ao salvar registro: {e}")


def _registry_add(share_id: str, cliente: str, periodo: str, xlsx_url: str | None, snapshot: dict | None = None, pwd_hash: str | None = None):
    """Adiciona entrada ao registro de forma thread-safe."""
    with _REGISTRY_LOCK:
        entries = _registry_load()
        entries.append({
            "share_id":  share_id,
            "cliente":   cliente,
            "periodo":   periodo,
            "xlsx_url":  xlsx_url,
            "criado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "url":       f"/d/{share_id}",
            "snapshot":  snapshot or {},
            "pwd_hash":  pwd_hash or "",
        })
        # Mantém apenas os últimos 500 para não crescer indefinidamente
        _registry_save(entries[-500:])


import secrets as _secrets
import string as _string

def _gerar_senha_dashboard() -> tuple[str, str]:
    """Gera senha alfanumérica maiúscula de 8 chars. Retorna (plain, hash)."""
    from werkzeug.security import generate_password_hash
    alfabeto = _string.ascii_uppercase + _string.digits
    senha = ''.join(_secrets.choice(alfabeto) for _ in range(8))
    return senha, generate_password_hash(senha, method="pbkdf2:sha256")


def _build_snapshot(relatorio: dict) -> dict:
    """Extrai KPIs essenciais do relatório para persistir no registry.json."""
    kpis = relatorio.get("kpis", {})
    com  = kpis.get("Com DigAI", {})
    sem  = kpis.get("Sem DigAI", {})
    roi  = relatorio.get("roi", {})
    return {
        "total_com":       com.get("total", 0),
        "total_sem":       sem.get("total", 0),
        "contratados_com": com.get("contratados", 0),
        "contratados_sem": sem.get("contratados", 0),
        "taxa_com":        round(float(com.get("taxa_contratacao", 0)) * 100, 2),
        "taxa_sem":        round(float(sem.get("taxa_contratacao", 0)) * 100, 2),
        "adesao_pct":      round(float(com.get("adesao", 0)) * 100, 2),
        "assertividade":   com.get("assertividade"),
        "sla_com":         com.get("sla_media"),
        "savings":         roi.get("savings", 0),
        "roi":             roi.get("roi", 0),
    }


def _get_cliente_historico(cliente: str, exclude_share_id: str) -> list:
    """Retorna snapshots históricos do cliente (excluindo o relatório atual)."""
    with _REGISTRY_LOCK:
        entries = _registry_load()
    historico = [
        {
            "periodo":   e["periodo"],
            "criado_em": e["criado_em"],
            "url":       e.get("url", f"/d/{e['share_id']}"),
            "share_id":  e["share_id"],
            **e.get("snapshot", {}),
        }
        for e in entries
        if e.get("cliente", "").strip().lower() == cliente.strip().lower()
        and e.get("share_id") != exclude_share_id
        and e.get("snapshot")
    ]
    return sorted(historico, key=lambda x: x.get("criado_em", ""))

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "digai-reports-secret-key-change-in-prod")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)
_max_mb = int(os.environ.get("MAX_UPLOAD_MB", 2048))
app.config["MAX_CONTENT_LENGTH"] = _max_mb * 1024 * 1024  # padrão 2 GB


# ── Merge de arquivos de upload ────────────────────────────────────────────────

def merge_upload_files(paths: list, field: str, out_dir: Path) -> str | None:
    """
    Concatena múltiplos arquivos (CSV ou XLSX) em um único CSV usando streaming.
    Nunca mantém mais de CHUNK_ROWS linhas em RAM por vez — evita OOM.

    Parameters
    ----------
    paths   : lista de caminhos de arquivo
    field   : nome do campo (ex: 'funnel') — usado no nome do arquivo merged
    out_dir : diretório onde salvar o merged

    Returns
    -------
    Caminho do arquivo merged, ou None se nenhuma linha foi lida.
    """
    import pandas as _pd
    import gc as _gc
    CHUNK = 5000

    if not paths:
        return None
    if len(paths) == 1:
        return paths[0]

    merged_path = str(out_dir / f"{field}_merged.csv")
    header_done = False
    total_rows  = 0

    for p in paths:
        ext = Path(p).suffix.lower()
        try:
            if ext in ('.xlsx', '.xls'):
                # Excel não suporta streaming — lê de uma vez (costuma ser menor)
                df_chunk = _pd.read_excel(p, dtype=str)
                df_chunk.to_csv(
                    merged_path,
                    mode='a' if header_done else 'w',
                    header=not header_done,
                    index=False, encoding='utf-8-sig',
                )
                total_rows += len(df_chunk)
                header_done = True
                del df_chunk; _gc.collect()
            else:
                # CSV: streaming por chunks
                read_ok = False
                for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
                    try:
                        first = True
                        for chunk in _pd.read_csv(
                            p, encoding=enc, sep=None, engine='python',
                            dtype=str, chunksize=CHUNK,
                        ):
                            chunk.to_csv(
                                merged_path,
                                mode='a' if (header_done or not first) else 'w',
                                header=(not header_done and first),
                                index=False, encoding='utf-8-sig',
                            )
                            if first:
                                header_done = True
                                first = False
                            total_rows += len(chunk)
                            del chunk
                        read_ok = True
                        break
                    except Exception:
                        continue
                if not read_ok:
                    print(f"[WARN] merge '{field}': não foi possível ler {Path(p).name}", flush=True)
        except Exception as e:
            print(f"[WARN] merge '{field}': {Path(p).name}: {e}", flush=True)

    print(f"[merge] '{field}': {len(paths)} arquivo(s) → {total_rows:,} linhas", flush=True)
    return merged_path if total_rows > 0 else None


# ── Limpeza de sessão ───────────────────────────────────────────────────────────

def _cleanup_dir(path: Path, delay: int = 5):
    """Deleta diretório em thread separada após `delay` segundos."""
    def _do():
        time.sleep(delay)
        shutil.rmtree(str(path), ignore_errors=True)
    threading.Thread(target=_do, daemon=True).start()


def _cleanup_all():
    """Zera uploads e relatórios pendentes (rodada diariamente às 03h)."""
    print("[cleanup] Limpeza diária iniciada...")
    for base in (UPLOAD_DIR, REPORTS_DIR):
        shutil.rmtree(str(base), ignore_errors=True)
        base.mkdir(exist_ok=True)
    # Purga dashboards compartilhados com mais de 30 dias (html + xlsx)
    cutoff = datetime.now() - timedelta(days=30)
    purged = 0
    for f in DASHBOARDS_DIR.glob("*.html"):
        try:
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink(missing_ok=True)
                xlsx_pair = f.with_suffix(".xlsx")
                xlsx_pair.unlink(missing_ok=True)
                purged += 1
        except Exception:
            pass
    gc.collect()
    print(f"[cleanup] Limpeza diária concluída. Dashboards expirados removidos: {purged}")


def _run_daily_scheduler():
    try:
        import schedule
        schedule.every().day.at("03:00").do(_cleanup_all)
        while True:
            schedule.run_pending()
            time.sleep(60)
    except ImportError:
        pass  # schedule não instalado — limpeza diária desabilitada


threading.Thread(target=_run_daily_scheduler, daemon=True).start()


# ── HTML da interface ──────────────────────────────────────────────────────────

PAGE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>DigAI Reports Engine</title>
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&display=swap" rel="stylesheet"/>
  <style>
    :root {
      --navy: #0D1B3E; --blue: #1B4FD8; --teal: #00CAF3;
      --bg: #06101f; --card: #0d1d35; --border: rgba(0,202,243,0.15);
      --text: #f0f4f8; --muted: #8ba3c4; --radius: 10px;
      --success: #20BD5A;
    }
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'Manrope',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}

    header{
      background:linear-gradient(135deg,#081b49 0%,#0034ab 45%,#00caf3 80%,#001c5d 100%);
      padding:0 2rem;height:64px;display:flex;align-items:center;
      justify-content:space-between;position:sticky;top:0;z-index:10;
      border-bottom:1px solid rgba(0,202,243,0.25);
    }
    .logo{font-weight:700;font-size:1.4rem;color:#fff;letter-spacing:-0.5px}
    .logo span{color:var(--teal)}
    .tag{font-size:0.72rem;color:rgba(255,255,255,0.5);padding-left:1rem;border-left:1px solid rgba(255,255,255,0.2);margin-left:1rem}

    main{max-width:860px;margin:0 auto;padding:2.5rem 1.5rem 4rem}

    h2{font-size:1.1rem;font-weight:700;margin-bottom:1.25rem;color:#fff}
    .sub{font-size:0.82rem;color:var(--muted);margin-bottom:2rem}

    .card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:1.75rem 2rem;margin-bottom:1.5rem}
    .card-title{font-size:0.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--teal);margin-bottom:1.25rem;display:flex;align-items:center;gap:0.5rem}
    .card-title::after{content:'';flex:1;height:1px;background:var(--border)}

    .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
    @media(max-width:600px){.form-grid{grid-template-columns:1fr}}

    .field{display:flex;flex-direction:column;gap:0.35rem}
    .field label{font-size:0.78rem;font-weight:600;color:var(--muted)}
    .field input,.field select{
      background:#112240;border:1px solid rgba(0,202,243,0.2);border-radius:6px;
      color:var(--text);font-family:'Manrope',sans-serif;font-size:0.88rem;
      padding:0.55rem 0.75rem;outline:none;transition:border-color 0.2s
    }
    .field input:focus,.field select:focus{border-color:var(--teal)}
    .field input[type=file]{padding:0.4rem 0.6rem;cursor:pointer}
    .field .hint{font-size:0.7rem;color:var(--muted);margin-top:0.2rem}
    .field input[type=file]::-webkit-file-upload-button{
      background:rgba(0,202,243,0.15);border:1px solid rgba(0,202,243,0.3);
      color:var(--teal);border-radius:4px;padding:0.25rem 0.75rem;
      font-family:'Manrope',sans-serif;font-size:0.8rem;cursor:pointer;margin-right:0.75rem
    }

    .optional{opacity:0.6}
    .optional label::after{content:' (opcional)';font-weight:400;font-size:0.7rem}

    .btn{
      width:100%;padding:0.85rem;background:linear-gradient(135deg,var(--blue),#0ea5e9);
      color:#fff;border:none;border-radius:8px;font-family:'Manrope',sans-serif;
      font-weight:700;font-size:1rem;cursor:pointer;transition:opacity 0.2s;margin-top:0.5rem
    }
    .btn:hover{opacity:0.9}
    .btn:disabled{opacity:0.5;cursor:wait}

    .progress{display:none;text-align:center;padding:2rem;color:var(--muted)}
    .spinner{width:36px;height:36px;border:3px solid rgba(0,202,243,0.2);
      border-top-color:var(--teal);border-radius:50%;animation:spin 0.8s linear infinite;margin:0 auto 1rem}
    @keyframes spin{to{transform:rotate(360deg)}}

    .result-card{
      background:linear-gradient(135deg,rgba(22,101,52,0.15),rgba(0,202,243,0.05));
      border:1px solid rgba(32,189,90,0.3);border-radius:var(--radius);
      padding:1.75rem 2rem;display:none;
    }
    .result-title{font-size:1.1rem;font-weight:700;color:var(--success);margin-bottom:1rem}
    .result-links{display:flex;flex-direction:column;gap:0.75rem;margin-top:1rem}
    .result-link{
      display:flex;align-items:center;gap:0.75rem;padding:0.75rem 1rem;
      background:rgba(255,255,255,0.04);border:1px solid var(--border);
      border-radius:6px;text-decoration:none;color:var(--text);font-size:0.9rem;
      transition:background 0.2s
    }
    .result-link:hover{background:rgba(0,202,243,0.08)}
    .result-link .icon{font-size:1.2rem}
    .result-link .label{flex:1;font-weight:600}
    .result-link .sub-label{font-size:0.72rem;color:var(--muted)}

    .kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-top:1.25rem}
    .kpi-mini{background:rgba(255,255,255,0.04);border:1px solid var(--border);border-radius:8px;padding:0.85rem;text-align:center}
    .kpi-mini .val{font-size:1.4rem;font-weight:700;color:var(--teal)}
    .kpi-mini .lbl{font-size:0.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-top:0.2rem}

    .error-card{background:rgba(220,38,38,0.1);border:1px solid rgba(220,38,38,0.3);
      border-radius:var(--radius);padding:1.5rem 2rem;display:none;color:#fca5a5}

    /* ── File list UI ── */
    .file-drop-area{
      background:#112240;border:1px dashed rgba(0,202,243,0.3);border-radius:6px;
      padding:0.5rem 0.75rem;cursor:pointer;transition:border-color 0.2s;
      display:flex;align-items:center;gap:0.5rem;min-height:2.4rem
    }
    .file-drop-area:hover{border-color:var(--teal)}
    .file-drop-area input[type=file]{
      position:absolute;width:0;height:0;opacity:0;pointer-events:none
    }
    .file-drop-label{font-size:0.82rem;color:var(--muted);flex:1}
    .file-drop-btn{
      background:rgba(0,202,243,0.15);border:1px solid rgba(0,202,243,0.3);color:var(--teal);
      border-radius:4px;padding:0.2rem 0.65rem;font-size:0.75rem;cursor:pointer;
      font-family:'Manrope',sans-serif;white-space:nowrap;flex-shrink:0
    }
    .file-tags{display:flex;flex-direction:column;gap:0.3rem;margin-top:0.4rem}
    .file-tag{
      display:flex;align-items:center;gap:0.5rem;
      background:rgba(0,202,243,0.07);border:1px solid rgba(0,202,243,0.18);
      border-radius:5px;padding:0.3rem 0.6rem;font-size:0.75rem
    }
    .file-tag-name{flex:1;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
    .file-tag-size{color:var(--muted);flex-shrink:0;font-size:0.68rem}
    .file-tag-rm{
      flex-shrink:0;background:none;border:none;color:rgba(220,38,38,0.7);cursor:pointer;
      font-size:0.9rem;line-height:1;padding:0 0.1rem;font-family:inherit
    }
    .file-tag-rm:hover{color:#ef4444}
    .error-card pre{font-size:0.78rem;white-space:pre-wrap;margin-top:0.75rem;opacity:0.8}
  </style>
</head>
<body>

<header>
  <div style="display:flex;align-items:center">
    <div class="logo">Dig<span>AI</span></div>
    <div class="tag">Reports Engine · Interface Local</div>
  </div>
  <div style="font-size:0.75rem;color:rgba(255,255,255,0.4)">localhost:5000</div>
</header>

<main>
  <h2>Gerador de Relatórios</h2>
  <p class="sub">Faça upload da base DigAI e, opcionalmente, do relatório de etapas do ATS. O relatório é gerado com os dados disponíveis.</p>

  <form id="form">

    <!-- Arquivos -->
    <div class="card">
      <div class="card-title">📂 Arquivos de Dados</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:1rem">
        A <strong style="color:var(--text)">base DigAI</strong> é o único arquivo obrigatório.
        Funciona com <strong style="color:var(--text)">qualquer ATS</strong> — Gupy, Kenoby, Breezy, Greenhouse, etc. —
        ou sem ATS. Quanto mais dados disponíveis, mais completo o relatório.
      </p>
      <div class="form-grid">
        <div class="field optional">
          <label>Relatório de Etapas do Processo</label>
          <div class="file-drop-area" onclick="document.getElementById('inp-funnel').click()">
            <input type="file" id="inp-funnel" name="funnel" accept=".csv,.xlsx,.xls" multiple/>
            <span class="file-drop-label" id="lbl-funnel">Nenhum arquivo selecionado</span>
            <span class="file-drop-btn">Selecionar</span>
          </div>
          <div class="file-tags" id="tags-funnel"></div>
          <span class="hint">Um ou mais arquivos · Export do ATS com etapas, status e datas · habilita SLA, funil e assertividade</span>
        </div>
        <div class="field optional">
          <label>Relatório de Candidaturas (complementar)</label>
          <div class="file-drop-area" onclick="document.getElementById('inp-candidatura').click()">
            <input type="file" id="inp-candidatura" name="candidatura" accept=".csv,.xlsx,.xls" multiple/>
            <span class="file-drop-label" id="lbl-candidatura">Nenhum arquivo selecionado</span>
            <span class="file-drop-btn">Selecionar</span>
          </div>
          <div class="file-tags" id="tags-candidatura"></div>
          <span class="hint">Um ou mais arquivos · telefone/email dos candidatos</span>
        </div>
        <div class="field">
          <label>Base DigAI — Entrevistas Realizadas ✱</label>
          <div class="file-drop-area" onclick="document.getElementById('inp-digai').click()">
            <input type="file" id="inp-digai" name="digai" accept=".csv,.xlsx,.xls" multiple required/>
            <span class="file-drop-label" id="lbl-digai">Nenhum arquivo selecionado</span>
            <span class="file-drop-btn">Selecionar</span>
          </div>
          <div class="file-tags" id="tags-digai"></div>
          <span class="hint">Um ou mais arquivos · define quem é "Com DigAI" pelo chaveamento</span>
        </div>
        <div class="field optional">
          <label>Logo do Cliente</label>
          <input type="file" id="inp-logo" name="logo" accept="image/*"/>
          <span class="hint">PNG/JPG · aparece no dashboard e no Excel</span>
        </div>
      </div>
    </div>

    <!-- Parâmetros -->
    <div class="card">
      <div class="card-title">⚙️ Parâmetros do Relatório</div>
      <div class="form-grid">
        <div class="field">
          <label>Nome do Cliente</label>
          <input type="text" name="cliente" placeholder="ex: Conta Simples" required/>
        </div>
        <div class="field">
          <label>Período</label>
          <input type="text" name="periodo" placeholder="ex: 07/2025 a 03/2026"/>
        </div>
        <div class="field">
          <label>Mensalidade DigAI (R$)</label>
          <input type="number" name="mensalidade" value="7600" step="100"/>
        </div>
        <div class="field">
          <label>Salário TA CLT (R$)</label>
          <input type="number" name="salario_ta" value="4750" step="100"/>
        </div>
        <div class="field">
          <label>Duração EI presencial (min)</label>
          <input type="number" name="tempo_ei" value="30" step="5"/>
        </div>
        <div class="field">
          <label>Cap. máx. TA (entrevistas/mês)</label>
          <input type="number" name="max_ta" value="127"/>
        </div>
        <div class="field">
          <label>Produtividade do recrutador</label>
          <input type="number" name="produtividade" value="0.60" step="0.05" min="0" max="1"/>
        </div>
        <div class="field">
          <label>Tipo de relatório</label>
          <select name="tipo_relatorio">
            <option value="consolidado">Consolidado (visão geral)</option>
            <option value="segmentado">Segmentado (separar por dimensão)</option>
            <option value="ambos">Ambos</option>
          </select>
        </div>
        <div class="field">
          <label>Granularidade temporal</label>
          <select name="granularidade">
            <option value="todos">Todos os dados (sem corte)</option>
            <option value="mensal">Mensal</option>
            <option value="quinzenal">Quinzenal</option>
            <option value="semanal">Semanal</option>
          </select>
        </div>
        <div class="field" id="field-data-inicio">
          <label>Data de início (corte)</label>
          <input type="date" name="data_inicio" />
        </div>
        <div class="field" id="field-data-fim">
          <label>Data de fim (corte)</label>
          <input type="date" name="data_fim" />
        </div>
      </div>
    </div>

    <!-- Segmentação (aparece quando tipo = segmentado ou ambos) -->
    <div class="card" id="card-segmentacao" style="display:none">
      <div class="card-title">📐 Configuração de Segmentação</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:1rem">
        As dimensões disponíveis são detectadas automaticamente após o upload.
        Selecione como deseja segmentar o relatório.
      </p>
      <div class="form-grid">
        <div class="field">
          <label>Segmentar por</label>
          <select name="dimensao">
            <option value="">Detectar automaticamente</option>
            <option value="area">Área da vaga</option>
            <option value="filial">Filial / Unidade</option>
            <option value="recrutador">Recrutador responsável</option>
            <option value="cargo">Cargo / Função</option>
            <option value="vaga">Nome da vaga</option>
            <option value="periodo">Período mensal</option>
          </select>
        </div>
        <div class="field optional">
          <label>Filtrar segmentos específicos</label>
          <input type="text" name="segmentos" placeholder="ex: SAC, Retenção, Financeiro"/>
          <span class="hint">Deixe vazio para incluir todos · separe por vírgula</span>
        </div>
      </div>
    </div>

    <script>
      document.querySelector('[name=tipo_relatorio]').addEventListener('change', function() {
        const card = document.getElementById('card-segmentacao');
        card.style.display = (this.value === 'segmentado' || this.value === 'ambos') ? 'block' : 'none';
      });
    </script>

    <div style="display:flex;gap:0.75rem">
      <button type="submit" class="btn" id="btn-submit" style="flex:1">⚡ Gerar Relatório</button>
      <button type="button" class="btn" id="btn-diag"
        style="flex:0 0 auto;width:auto;padding-left:1.5rem;padding-right:1.5rem;
               background:linear-gradient(135deg,#1a3a5c,#0f2847);border:1px solid rgba(0,202,243,0.3)"
        title="Inspeciona os arquivos e mostra colunas detectadas + contagem de matches">
        🔍 Diagnóstico
      </button>
    </div>
  </form>

  <div class="card" id="diag-card" style="display:none;margin-top:1rem">
    <div class="card-title">🔍 Resultado do Diagnóstico</div>
    <div id="diag-out"></div>
  </div>

  <style>
    .diag-section{background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:8px;padding:1rem 1.25rem;margin-bottom:0.75rem}
    .diag-section-title{font-size:0.7rem;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--teal);margin-bottom:0.6rem}
    .diag-row{display:flex;align-items:baseline;gap:0.5rem;font-size:0.8rem;margin-bottom:0.3rem}
    .diag-label{color:var(--muted);min-width:130px;flex-shrink:0}
    .diag-value{color:var(--text);font-weight:600}
    .diag-cols{display:flex;flex-wrap:wrap;gap:0.3rem;margin-top:0.5rem}
    .diag-col-tag{background:rgba(0,202,243,0.08);border:1px solid rgba(0,202,243,0.2);border-radius:4px;padding:0.15rem 0.5rem;font-size:0.68rem;color:var(--muted)}
    .diag-col-tag.highlight{background:rgba(0,202,243,0.18);border-color:rgba(0,202,243,0.5);color:var(--teal)}
    .diag-emails{font-size:0.72rem;color:var(--muted);margin-top:0.3rem;word-break:break-all}
    .diag-match-box{background:rgba(32,189,90,0.08);border:1px solid rgba(32,189,90,0.3);border-radius:8px;padding:1rem 1.25rem;margin-bottom:0.75rem}
    .diag-match-box.warn{background:rgba(245,158,11,0.08);border-color:rgba(245,158,11,0.3)}
    .diag-match-box.danger{background:rgba(220,38,38,0.08);border-color:rgba(220,38,38,0.3)}
    .diag-match-num{font-size:1.8rem;font-weight:700;color:var(--success);line-height:1}
    .diag-match-num.warn{color:#f59e0b}
    .diag-match-num.danger{color:#ef4444}
    .diag-badge{display:inline-block;padding:0.15rem 0.5rem;border-radius:4px;font-size:0.68rem;font-weight:700;margin-left:0.35rem}
    .badge-ok{background:rgba(32,189,90,0.15);color:var(--success)}
    .badge-warn{background:rgba(245,158,11,0.15);color:#f59e0b}
    .badge-err{background:rgba(220,38,38,0.15);color:#ef4444}
    .diag-note{font-size:0.72rem;color:var(--muted);margin-top:0.5rem;font-style:italic}
  </style>

  <div class="progress" id="progress">
    <div class="spinner"></div>
    <p id="progress-msg">Processando os arquivos…</p>
    <p style="font-size:0.75rem;margin-top:0.5rem;opacity:0.6">Arquivos grandes podem demorar 30–60s</p>
  </div>

  <div class="result-card" id="result">
    <div class="result-title">✅ Relatório gerado com sucesso!</div>
    <div class="kpi-row" id="kpi-row"></div>
    <div id="alertas-qualidade" style="margin-top:1rem"></div>
    <div class="result-links" id="result-links"></div>
    <div id="share-box" style="display:none;margin-top:1.25rem;background:rgba(0,202,243,0.06);border:1px solid rgba(0,202,243,0.25);border-radius:8px;padding:0.9rem 1rem">
      <div style="font-size:0.72rem;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--teal);margin-bottom:0.5rem">
        🔗 Link compartilhável — válido por 30 dias
      </div>
      <div style="display:flex;gap:0.5rem;align-items:center">
        <input id="share-url-input" type="text" readonly
          style="flex:1;background:rgba(0,0,0,0.3);border:1px solid rgba(0,202,243,0.2);border-radius:6px;
                 padding:0.45rem 0.75rem;font-size:0.8rem;color:var(--text);font-family:monospace;min-width:0"/>
        <button id="btn-copy-share" onclick="copyShareUrl()"
          style="flex-shrink:0;background:rgba(0,202,243,0.15);border:1px solid rgba(0,202,243,0.4);
                 color:var(--teal);border-radius:6px;padding:0.45rem 0.9rem;font-size:0.8rem;cursor:pointer;
                 font-family:inherit;white-space:nowrap">
          Copiar
        </button>
      </div>
      <div style="font-size:0.71rem;color:var(--muted);margin-top:0.4rem">
        O dashboard ficará disponível neste link mesmo após o Excel ser baixado.
      </div>
    </div>
  </div>

  <div class="error-card" id="error-card">
    <strong>❌ Erro ao processar</strong>
    <pre id="error-msg"></pre>
  </div>

</main>

<script>
const form = document.getElementById('form');
const btn  = document.getElementById('btn-submit');

// ── File list UI ────────────────────────────────────────────────────────────

function fmtSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024*1024) return (bytes/1024).toFixed(1) + ' KB';
  return (bytes/(1024*1024)).toFixed(1) + ' MB';
}

function renderFileTags(field) {
  const inp   = document.getElementById('inp-' + field);
  const tags  = document.getElementById('tags-' + field);
  const lbl   = document.getElementById('lbl-' + field);
  const files = Array.from(inp.files);

  if (!files.length) {
    lbl.textContent = 'Nenhum arquivo selecionado';
    tags.innerHTML  = '';
    return;
  }
  lbl.textContent = files.length === 1
    ? files[0].name
    : `${files.length} arquivo(s) selecionado(s)`;

  tags.innerHTML = files.map((f, i) => `
    <div class="file-tag" id="ftag-${field}-${i}">
      <span class="file-tag-status" style="font-size:0.75rem;flex-shrink:0;width:1rem;text-align:center">⏸</span>
      <span class="file-tag-name" title="${f.name}">${f.name}</span>
      <span class="file-tag-size">${fmtSize(f.size)}</span>
      <button type="button" class="file-tag-rm" title="Remover" onclick="removeFile('${field}',${i})">✕</button>
    </div>`).join('');
}

function removeFile(field, idx) {
  const inp = document.getElementById('inp-' + field);
  const dt  = new DataTransfer();
  Array.from(inp.files).forEach((f, i) => { if (i !== idx) dt.items.add(f); });
  inp.files = dt.files;
  renderFileTags(field);
}

['funnel','candidatura','digai'].forEach(field => {
  document.getElementById('inp-' + field)
    .addEventListener('change', () => renderFileTags(field));
});

function copyShareUrl() {
  const inp = document.getElementById('share-url-input');
  inp.select();
  inp.setSelectionRange(0, 99999);
  try {
    navigator.clipboard.writeText(inp.value).then(() => {
      const btn = document.getElementById('btn-copy-share');
      btn.textContent = '✓ Copiado';
      setTimeout(() => { btn.textContent = 'Copiar'; }, 2000);
    });
  } catch(e) {
    document.execCommand('copy');
  }
}

document.getElementById('btn-diag').addEventListener('click', async () => {
  document.getElementById('diag-card').style.display = 'block';
  document.getElementById('diag-out').innerHTML = '<p style="color:var(--muted);font-size:0.82rem">Enviando arquivos…</p>';
  try {
    // Upload arquivos individualmente (evita limite de proxy)
    const session_id = await uploadAllFiles(msg => {
      document.getElementById('diag-out').innerHTML =
        `<p style="color:var(--muted);font-size:0.82rem">${msg}</p>`;
    });
    document.getElementById('diag-out').innerHTML =
      '<p style="color:var(--muted);font-size:0.82rem">Analisando dados…</p>';
    const fd = new FormData();
    fd.append('session_id', session_id);
    const res = await fetchRetry('/diagnostico', {method:'POST', body: fd}, 3, 3000);
    const ct2 = res.headers.get('content-type') || '';
    if (!ct2.includes('application/json')) {
      const txt = await res.text();
      throw new Error(`Servidor retornou HTTP ${res.status}.\n\n${txt.slice(0,300)}`);
    }
    const data = await res.json();
    const out = document.getElementById('diag-out');
    if (data.error) {
      out.innerHTML = `<pre style="color:#fca5a5;font-size:0.75rem;white-space:pre-wrap">${data.error}</pre>`;
      return;
    }

    function colTags(cols, highlights=[]) {
      const hl = new Set(highlights.map(h => h.toLowerCase()));
      return cols.map(c => {
        const isHl = hl.has(c.toLowerCase()) || hl.has(c.replace(/_/g,' ').toLowerCase());
        return `<span class="diag-col-tag${isHl?' highlight':''}">${c}</span>`;
      }).join('');
    }

    function row(label, value) {
      return `<div class="diag-row"><span class="diag-label">${label}</span><span class="diag-value">${value}</span></div>`;
    }

    function badge(ok, warn, err) {
      if (ok)   return `<span class="diag-badge badge-ok">✓ OK</span>`;
      if (warn) return `<span class="diag-badge badge-warn">⚠ ${warn}</span>`;
      return `<span class="diag-badge badge-err">✗ ${err}</span>`;
    }

    let html = '';

    // ── Arquivo de Etapas ──────────────────────────────────────────────
    if (data.funnel) {
      const f = data.funnel;
      const noEmails = f.emails_com_valor === 0;
      html += `<div class="diag-section">
        <div class="diag-section-title">📂 Arquivo de Etapas (ATS)</div>
        ${row('Registros', f.rows.toLocaleString('pt-BR'))}
        ${row('Email detectado', f.email_found
          ? `✅ Sim ${badge(f.emails_com_valor > 0, null, 'sem valores')}`
          : `❌ Não`)}
        ${row('Emails com valor', f.emails_com_valor.toLocaleString('pt-BR'))}
        ${noEmails ? `<div class="diag-note">⚠️ Relatório agregado por vaga (sem emails individuais) — candidaturas será a base primária para o join</div>` : ''}
        ${f.sample_emails && f.sample_emails.length ? `<div class="diag-emails">Amostra: ${f.sample_emails.join(' · ')}</div>` : ''}
        <div class="diag-cols" style="margin-top:0.6rem">
          ${colTags(f.columns, ['email','e-mail','status','etapa_atual','data_cadastro','data_final','data_contratacao'])}
        </div>
      </div>`;
    }

    // ── Candidaturas ───────────────────────────────────────────────────
    if (data.candidatura) {
      const c = data.candidatura;
      html += `<div class="diag-section">
        <div class="diag-section-title">📋 Candidaturas / Contratações (complementar)</div>
        ${row('Registros', c.rows.toLocaleString('pt-BR'))}
        ${row('Emails com valor', `${c.emails_com_valor.toLocaleString('pt-BR')} ${badge(c.emails_com_valor > 0, null, 'sem emails')}`)}
        ${c.contratados != null ? row('Contratados detectados', `${c.contratados.toLocaleString('pt-BR')} ${badge(c.contratados > 0, c.contratados === 0 ? 'nenhum' : null, null)}`) : ''}
        ${c.sample_emails && c.sample_emails.length ? `<div class="diag-emails">Amostra: ${c.sample_emails.join(' · ')}</div>` : ''}
        <div class="diag-cols" style="margin-top:0.6rem">
          ${colTags(c.columns, ['email','e-mail','data_contratacao','tags','status','candidato_id','phone'])}
        </div>
      </div>`;
    }

    // ── Base DigAI ─────────────────────────────────────────────────────
    if (data.digai) {
      const d = data.digai;
      html += `<div class="diag-section">
        <div class="diag-section-title">🤖 Base DigAI</div>
        ${row('Entrevistas', d.rows.toLocaleString('pt-BR'))}
        ${row('Emails com valor', `${d.emails_com_valor.toLocaleString('pt-BR')} ${badge(d.emails_com_valor > 0, null, 'sem emails')}`)}
        ${row('Data EI', d.data_ei_found ? `✅ ${d.data_ei_nao_nulos.toLocaleString('pt-BR')} registros` : `⚠️ Não encontrada (join por email ainda funciona)`)}
        ${d.sample_emails && d.sample_emails.length ? `<div class="diag-emails">Amostra: ${d.sample_emails.join(' · ')}</div>` : ''}
        <div class="diag-cols" style="margin-top:0.6rem">
          ${colTags(d.columns, ['email','phone','aprovado_ia','score_ia','data_ei','hasapproved'])}
        </div>
      </div>`;
    }

    // ── Resultado do Join ──────────────────────────────────────────────
    if (data.match) {
      const m = data.match;
      const pct = m.emails_unicos_fonte > 0
        ? Math.round(m.matches_email / m.emails_unicos_fonte * 100) : 0;
      const cls = m.matches_email === 0 ? 'danger' : (pct < 10 ? 'warn' : '');
      const numCls = m.matches_email === 0 ? 'danger' : (pct < 10 ? 'warn' : '');

      html += `<div class="diag-match-box ${cls}">
        <div class="diag-section-title" style="color:${m.matches_email>0?'var(--success)':'#ef4444'}">🔗 Resultado do Join</div>
        ${row('Fonte dos emails', m.fonte === 'candidatura'
          ? '📋 Candidaturas (ATS sem emails individuais)'
          : '📂 Arquivo de Etapas')}
        ${row('Emails únicos na fonte', m.emails_unicos_fonte.toLocaleString('pt-BR'))}
        ${row('Emails únicos no DigAI', m.emails_unicos_digai.toLocaleString('pt-BR'))}
        <div style="margin-top:0.75rem;margin-bottom:0.4rem">
          <span class="diag-match-num ${numCls}">${m.matches_email.toLocaleString('pt-BR')}</span>
          <span style="font-size:0.82rem;color:var(--muted);margin-left:0.5rem">matches de email com a base DigAI
            ${m.emails_unicos_fonte > 0 ? `<strong style="color:var(--text)"> (${pct}%)</strong>` : ''}
          </span>
        </div>
        ${m.matches_email === 0 ? `<div class="diag-note">❌ Nenhum match — verifique se os emails do ATS e do DigAI usam o mesmo domínio/formato.</div>` : ''}
        ${m.sample_matches && m.sample_matches.length ? `<div class="diag-emails" style="margin-top:0.4rem">Amostra: ${m.sample_matches.join(' · ')}</div>` : ''}
      </div>`;

      // ── Contratados com DigAI ──────────────────────────────────────
      if (m.hired_total != null) {
        const hPct = m.hired_total > 0 ? Math.round(m.hired_in_digai / m.hired_total * 100) : 0;
        const hCls = m.hired_in_digai === 0 ? 'danger' : (hPct < 20 ? 'warn' : '');
        html += `<div class="diag-match-box ${hCls}">
          <div class="diag-section-title" style="color:var(--teal)">👔 Contratados que fizeram EI DigAI</div>
          ${row('Total de contratados', m.hired_total.toLocaleString('pt-BR'))}
          <div style="margin-top:0.5rem;margin-bottom:0.4rem">
            <span class="diag-match-num ${hCls}" style="${m.hired_in_digai>0?'color:var(--teal)':''}">${m.hired_in_digai.toLocaleString('pt-BR')}</span>
            <span style="font-size:0.82rem;color:var(--muted);margin-left:0.5rem">contratados com registro na base DigAI
              ${m.hired_total > 0 ? `<strong style="color:var(--text)"> (${hPct}%)</strong>` : ''}
            </span>
          </div>
          ${m.hired_in_digai === 0 && m.hired_total > 0
            ? `<div class="diag-note">⚠️ Nenhum contratado encontrado na base DigAI. Verifique se os emails batem — possível diferença de formato ou período.</div>`
            : ''}
          ${m.sample_hired_digai && m.sample_hired_digai.length ? `<div class="diag-emails" style="margin-top:0.4rem">Amostra: ${m.sample_hired_digai.join(' · ')}</div>` : ''}
        </div>`;
      }
    }

    out.innerHTML = html || '<p style="color:var(--muted)">Sem dados retornados.</p>';
  } catch(err) {
    document.getElementById('diag-out').innerHTML = `<pre style="color:#fca5a5;font-size:0.75rem">${String(err)}</pre>`;
  }
});

// ── Fetch com retry automático (502/503/504 = servidor reiniciando) ──────────

async function fetchRetry(url, opts, maxRetries=4, baseDelay=2000) {
  let lastErr;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await fetch(url, opts);
      // 502/503/504 = proxy/servidor indisponível → tenta novamente
      if (res.status === 502 || res.status === 503 || res.status === 504) {
        lastErr = new Error(`HTTP ${res.status}`);
        if (attempt < maxRetries - 1) {
          await new Promise(r => setTimeout(r, baseDelay * (attempt + 1)));
          continue;
        }
        return res; // retorna mesmo assim na última tentativa
      }
      return res;
    } catch(e) {
      lastErr = e;
      if (attempt < maxRetries - 1) {
        await new Promise(r => setTimeout(r, baseDelay * (attempt + 1)));
      }
    }
  }
  throw lastErr;
}

// ── Upload sequencial por arquivo ───────────────────────────────────────────

async function uploadAllFiles(onProgress) {
  const fields = ['funnel','candidatura','digai','logo'];
  const MAX_SESSION_RETRIES = 3;

  for (let sessionAttempt = 0; sessionAttempt < MAX_SESSION_RETRIES; sessionAttempt++) {
    // 1. Cria sessão (com retry — pode pegar cold start do Render)
    if (sessionAttempt > 0) {
      if (onProgress) onProgress(`Servidor reiniciado. Reenviando arquivos… (tentativa ${sessionAttempt + 1}/${MAX_SESSION_RETRIES})`);
      // Reseta ícones de status para ⏸
      for (const field of fields) {
        const tags = document.getElementById('tags-' + field);
        if (tags) {
          Array.from(tags.children).forEach(tagEl => {
            const st = tagEl.querySelector('.file-tag-status');
            if (st) st.textContent = '⏸';
          });
        }
      }
      await new Promise(r => setTimeout(r, 3000));
    } else {
      if (onProgress) onProgress('Conectando ao servidor…');
    }

    let session_id;
    for (let attempt = 0; attempt < 5; attempt++) {
      const initRes = await fetchRetry('/upload/init', {method:'POST'}, 4, 2500);
      if (initRes.ok) {
        const data = await initRes.json();
        session_id = data.session_id;
        break;
      }
      if (attempt === 4) throw new Error('Servidor indisponível. Aguarde alguns segundos e tente novamente.');
      if (onProgress) onProgress(`Servidor aquecendo… tentativa ${attempt+2}/5`);
      await new Promise(r => setTimeout(r, 3000));
    }
    if (!session_id) throw new Error('Falha ao criar sessão de upload.');

    // 2. Envia cada arquivo individualmente
    let sessionLost = false;
    outer: for (const field of fields) {
      const inp = document.getElementById(field === 'logo' ? 'inp-logo' : 'inp-' + field)
                || form.querySelector(`[name="${field}"]`);
      if (!inp) continue;
      const files = Array.from(inp.files || []);
      for (let i = 0; i < files.length; i++) {
        const f = files[i];
        if (onProgress) onProgress(`Enviando ${f.name}…`);
        const fd2 = new FormData();
        fd2.append('file', f);
        const r = await fetchRetry(`/upload/${session_id}/${field}`, {method:'POST', body: fd2}, 3, 2000);
        if (r.status === 404) {
          // Servidor reiniciou — sessão perdida, recomeça tudo
          sessionLost = true;
          break outer;
        }
        if (!r.ok) {
          const err = await r.json().catch(() => ({}));
          throw new Error(`Erro ao enviar ${f.name}: ${err.error || r.status}`);
        }
        // Atualiza tag de status
        const tags = document.getElementById('tags-' + field);
        if (tags) {
          const tagEl = tags.children[i];
          if (tagEl) {
            const st = tagEl.querySelector('.file-tag-status');
            if (st) st.textContent = '✓';
          }
        }
      }
    }

    if (!sessionLost) return session_id;
    // sessionLost — retry outer loop com nova sessão
  }

  throw new Error('Servidor reiniciou durante o upload 3 vezes seguidas. Tente novamente em alguns segundos.');
}

form.addEventListener('submit', async e => {
  e.preventDefault();
  btn.disabled = true;
  document.getElementById('progress').style.display = 'block';
  document.getElementById('result').style.display = 'none';
  document.getElementById('error-card').style.display = 'none';

  const msgs = [
    'Detectando etapas do funil…',
    'Classificando Com / Sem DigAI…',
    'Calculando KPIs e ROI…',
    'Gerando Excel com fórmulas auditáveis…',
    'Montando dashboard HTML…',
    'Quase pronto…'
  ];
  let mi = 0, tick = null;

  try {
    // Envia arquivos um a um antes de iniciar o processamento
    const session_id = await uploadAllFiles(msg => {
      document.getElementById('progress-msg').textContent = msg;
    });

    tick = setInterval(() => {
      document.getElementById('progress-msg').textContent = msgs[mi % msgs.length];
      mi++;
    }, 3500);

    // Envia apenas parâmetros + session_id (sem arquivos no body)
    const fd = new FormData();
    fd.append('session_id', session_id);
    ['cliente','periodo','mensalidade','salario_ta','tempo_ei',
     'max_ta','produtividade','tipo_relatorio','dimensao','segmentos'].forEach(n => {
      const el = form.querySelector(`[name="${n}"]`);
      if (el) fd.append(n, el.value);
    });
    const res = await fetchRetry('/gerar', {method:'POST', body: fd}, 3, 3000);
    const ct = res.headers.get('content-type') || '';
    if (!ct.includes('application/json')) {
      const txt = await res.text();
      throw new Error(`Servidor retornou HTTP ${res.status}. Tente novamente em alguns segundos.\n\n${txt.slice(0,300)}`);
    }
    const data = await res.json();
    clearInterval(tick);
    document.getElementById('progress').style.display = 'none';
    btn.disabled = false;

    if (data.error) {
      document.getElementById('error-msg').textContent = data.error;
      document.getElementById('error-card').style.display = 'block';
      return;
    }

    // KPIs
    const kpis = [
      {val: data.kpis_com_contratados, lbl: 'Contratações DigAI'},
      {val: data.roi_x + 'x', lbl: 'ROI'},
      {val: 'R$ ' + data.saving_fmt, lbl: 'Saving'},
      {val: data.sla_com != null ? data.sla_com + 'd' : '—', lbl: 'SLA Com DigAI'},
    ];
    document.getElementById('kpi-row').innerHTML = kpis.map(k =>
      `<div class="kpi-mini"><div class="val">${k.val}</div><div class="lbl">${k.lbl}</div></div>`
    ).join('');

    // Links
    const links = [];
    if (data.html_url) links.push({icon:'🌐', label:'Dashboard HTML', sub:'Abre no navegador', href: data.html_url});
    if (data.xlsx_url) links.push({icon:'📊', label:'Excel (5 abas, fórmulas)', sub:'Baixar relatorio.xlsx', href: data.xlsx_url});
    document.getElementById('result-links').innerHTML = links.map(l =>
      `<a href="${l.href}" target="_blank" class="result-link">
        <span class="icon">${l.icon}</span>
        <span class="label">${l.label}<br><span class="sub-label">${l.sub}</span></span>
        <span>→</span>
      </a>`
    ).join('');

    // Senha do dashboard (exibida UMA vez)
    if (data.senha_dashboard) {
      const senhaBox = document.createElement('div');
      senhaBox.style.cssText = 'background:rgba(37,99,235,0.1);border:1px solid rgba(37,99,235,0.4);border-radius:8px;padding:1rem 1.25rem;margin-top:0.75rem';
      senhaBox.innerHTML = `
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:#94a3b8;margin-bottom:0.5rem">Senha de acesso ao dashboard</div>
        <div style="display:flex;align-items:center;gap:0.75rem">
          <code style="font-size:1.4rem;font-weight:800;color:#00caf3;letter-spacing:0.2em;flex:1">${data.senha_dashboard}</code>
          <button onclick="navigator.clipboard.writeText('${data.senha_dashboard}');this.textContent='Copiada!';setTimeout(()=>this.textContent='Copiar',2000)"
            style="background:rgba(0,202,243,0.15);border:1px solid rgba(0,202,243,0.4);color:#00caf3;border-radius:6px;padding:0.35rem 0.75rem;font-size:0.75rem;font-weight:600;cursor:pointer;white-space:nowrap">Copiar</button>
        </div>
        <div style="font-size:0.72rem;color:#64748b;margin-top:0.5rem">Compartilhe esta senha com o destinatário do link. Ela não poderá ser recuperada depois.</div>`;
      document.getElementById('result-links').after(senhaBox);
    }

    // Erro de geração do Excel
    if (data.excel_error) {
      const exErr = document.createElement('div');
      exErr.style.cssText = 'background:rgba(192,0,0,0.12);border:1px solid rgba(192,0,0,0.4);border-radius:6px;padding:0.75rem 1rem;margin-top:0.75rem;font-size:0.75rem;color:#ffcccc';
      exErr.innerHTML = '<strong>⚠️ Excel não gerado — erro interno:</strong><br><details><summary style="cursor:pointer;margin-top:4px">Ver traceback</summary><pre style="margin-top:6px;font-size:0.7rem;white-space:pre-wrap;word-break:break-all">' + data.excel_error.replace(/</g,'&lt;') + '</pre></details>';
      document.getElementById('result-links').after(exErr);
    }

    // Link compartilhável
    if (data.share_url) {
      const fullUrl = window.location.origin + data.share_url;
      document.getElementById('share-url-input').value = fullUrl;
      document.getElementById('share-box').style.display = 'block';
    }

    // Alertas de qualidade (duplicatas + desalinhados) — uso interno
    const alertas = data.alertas_qualidade || [];
    const desalin = data.desalinhados || [];
    let alertasHtml = '';
    if (alertas.length) {
      alertasHtml += alertas.map(a => `
        <div style="background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.3);
          border-radius:6px;padding:0.75rem 1rem;margin-bottom:0.5rem;font-size:0.8rem;color:#fef3c7">
          ${a}
        </div>`).join('');
    }
    if (desalin.length) {
      alertasHtml += `<details style="margin-top:0.5rem">
        <summary style="font-size:0.78rem;color:var(--muted);cursor:pointer">
          🔍 Ver ${desalin.length} contratado(s) na base DigAI classificados como Sem DigAI
        </summary>
        <table style="width:100%;font-size:0.72rem;border-collapse:collapse;margin-top:0.5rem">
          <tr style="background:rgba(0,202,243,0.1)">
            <th style="padding:4px 8px;text-align:left">Email</th>
            <th style="padding:4px 8px;text-align:left">Nome</th>
            <th style="padding:4px 8px;text-align:left">Vaga</th>
          </tr>
          ${desalin.slice(0,50).map(d=>`<tr style="border-top:1px solid rgba(255,255,255,0.05)">
            <td style="padding:4px 8px;color:var(--muted)">${d.email||'—'}</td>
            <td style="padding:4px 8px">${d.nome||'—'}</td>
            <td style="padding:4px 8px">${d.vaga||'—'}</td>
          </tr>`).join('')}
        </table>
      </details>`;
    }
    document.getElementById('alertas-qualidade').innerHTML = alertasHtml;

    document.getElementById('result').style.display = 'block';
  } catch(err) {
    clearInterval(tick);
    document.getElementById('progress').style.display = 'none';
    btn.disabled = false;
    document.getElementById('error-msg').textContent = String(err);
    document.getElementById('error-card').style.display = 'block';
  }
});
</script>
</body>
</html>
"""


# ── Rotas ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(PAGE)


@app.route("/ping")
def ping():
    """Health-check leve — usado pelo JS para aguardar o servidor acordar."""
    return jsonify({"ok": True})


@app.route("/upload/init", methods=["POST"])
def upload_init():
    """Cria sessão de upload e retorna session_id."""
    session_id = uuid.uuid4().hex[:8]
    tmp_dir = UPLOAD_DIR / session_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return jsonify({"session_id": session_id})


@app.route("/upload/<session_id>/<field>", methods=["POST"])
def upload_file(session_id, field):
    """Recebe um único arquivo para a sessão. Requests pequenos — sem limite de proxy."""
    if not re.match(r'^[0-9a-f]{8}$', session_id):
        return jsonify({"error": "sessão inválida"}), 400
    if field not in ('funnel', 'candidatura', 'digai', 'logo'):
        return jsonify({"error": "campo inválido"}), 400

    tmp_dir = UPLOAD_DIR / session_id
    if not tmp_dir.exists():
        return jsonify({"error": "sessão não encontrada"}), 404

    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({"error": "nenhum arquivo enviado"}), 400

    try:
        # Índice sequencial por campo
        tmp_dir.mkdir(parents=True, exist_ok=True)  # recria se apagado
        existing = len(list(tmp_dir.glob(f"{field}_*")))
        ext  = Path(f.filename).suffix or ".csv"
        path = tmp_dir / f"{field}_{existing}{ext}"
        f.save(str(path))
        return jsonify({"ok": True, "filename": f.filename, "size": path.stat().st_size})
    except Exception:
        import traceback
        tb = traceback.format_exc()
        print(f"[ERROR /upload] {session_id}/{field} — {tb}", flush=True)
        return jsonify({"error": tb}), 500


@app.route("/gerar", methods=["POST"])
def gerar():
    import base64

    # ── Resolver sessão de upload ──────────────────────────────────────────────
    # Quando session_id é enviado, os arquivos já estão no disco (upload por arquivo).
    # Fallback: arquivos enviados diretamente no body (compatibilidade).
    sid = request.form.get("session_id", "").strip()
    if sid and re.match(r'^[0-9a-f]{8}$', sid):
        session_id = sid
        tmp_dir = UPLOAD_DIR / session_id
        if not tmp_dir.exists():
            return jsonify({"error": "Sessão de upload não encontrada. Recarregue a página."}), 400

        def _list(field):
            # Exclui arquivos _merged para não duplicar se a sessão for reutilizada
            return sorted([
                str(p) for p in tmp_dir.glob(f"{field}_*")
                if "_merged" not in p.name
            ]) or None

        funnel_paths = _list("funnel")
        cand_paths   = _list("candidatura")
        digai_paths  = _list("digai")
        logo_files   = sorted(f for f in tmp_dir.glob("logo_*") if "_merged" not in f.name)
        logo_path    = str(logo_files[0]) if logo_files else None
    else:
        # Fallback: arquivos no body (apenas para arquivos pequenos / testes)
        session_id = uuid.uuid4().hex[:8]
        tmp_dir = UPLOAD_DIR / session_id
        tmp_dir.mkdir(parents=True)

        def _save_multi(field):
            files, paths = request.files.getlist(field), []
            for i, f in enumerate(files):
                if f and f.filename:
                    ext = Path(f.filename).suffix or ".csv"
                    p = tmp_dir / f"{field}_{i}{ext}"; f.save(str(p)); paths.append(str(p))
            return paths or None

        def _save_one(field):
            f = request.files.get(field)
            if f and f.filename:
                ext = Path(f.filename).suffix or ".csv"
                p = tmp_dir / f"{field}_0{ext}"; f.save(str(p)); return str(p)
            return None

        funnel_paths = _save_multi("funnel")
        cand_paths   = _save_multi("candidatura")
        digai_paths  = _save_multi("digai")
        logo_path    = _save_one("logo")

    try:
        funnel_path      = merge_upload_files(funnel_paths, "funnel",      tmp_dir)
        candidatura_path = merge_upload_files(cand_paths,   "candidatura", tmp_dir)
        digai_path       = merge_upload_files(digai_paths,  "digai",       tmp_dir)

        # Apenas a base DigAI é obrigatória — funil e candidatura são opcionais
        if not digai_path:
            return jsonify({"error": "Base DigAI é obrigatória. Envie o export da plataforma DigAI com as entrevistas realizadas."}), 400

        # ── Parâmetros ────────────────────────────────────────────────────────
        periodo_str = request.form.get("periodo", "")

        # n_meses: se informado explicitamente via form, passa para analytics.
        # Caso contrário, analytics._infer_n_meses() infere a partir do campo
        # "periodo" (string "MM/YYYY a MM/YYYY") e do range de data_cadastro do df.
        # Toda a lógica de inferência fica em um único lugar: analytics.py.
        data_inicio_str = request.form.get("data_inicio", "").strip()
        data_fim_str    = request.form.get("data_fim",    "").strip()
        granularidade   = request.form.get("granularidade", "todos").strip()

        params = {
            "cliente_nome":         request.form.get("cliente", "Cliente"),
            "periodo":              periodo_str,
            "mensalidade_digai":    float(request.form.get("mensalidade", 7600)),
            "salario_ta_clt":       float(request.form.get("salario_ta", 4750)),
            "tempo_entrevista_min": int(request.form.get("tempo_ei", 30)),
            "max_entrevistas_ta":   int(request.form.get("max_ta", 127)),
            "produtividade_pct":    float(request.form.get("produtividade", 0.60)),
            "logo_url":             "",
            "data_inicio":          data_inicio_str,
            "data_fim":             data_fim_str,
            "granularidade":        granularidade,
        }
        n_meses_form = request.form.get("n_meses", "").strip()
        if n_meses_form.isdigit() and int(n_meses_form) > 0:
            params["n_meses"] = int(n_meses_form)

        # Logo em base64 para embutir no HTML
        if logo_path:
            import mimetypes
            mime, _ = mimetypes.guess_type(logo_path)
            with open(logo_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            params["logo_url"] = f"data:{mime};base64,{b64}"

        # ── Gerar relatório via orquestrador central ──────────────────────────
        # Nova assinatura v3: digai_path obrigatório, funnel_path opcional
        from engine.analytics import analisar_qualidade
        relatorio, _pipeline_df = _pipeline_run(
            digai_path=digai_path,
            funnel_path=funnel_path,
            candidatura_path=candidatura_path,
            params=params,
            session_id=session_id,
        )

        # ── Filtro de corte por data (granularidade / data_inicio / data_fim) ──
        if "data_cadastro" in _pipeline_df.columns and (params.get("data_inicio") or params.get("data_fim")):
            import pandas as _pd_cut
            _dt = _pd_cut.to_datetime(_pipeline_df["data_cadastro"], errors="coerce")
            if params.get("data_inicio"):
                try:
                    _dt_ini = _pd_cut.Timestamp(params["data_inicio"])
                    _mask = _dt >= _dt_ini
                    if _mask.any():
                        _pipeline_df = _pipeline_df[_mask].copy()
                        _dt = _dt[_mask]
                        print(f"[gerar] Corte data_inicio {params['data_inicio']}: {len(_pipeline_df):,} candidatos restantes")
                except Exception as _e:
                    print(f"[WARN] data_inicio inválida: {_e}")
            if params.get("data_fim"):
                try:
                    _dt_fim = _pd_cut.Timestamp(params["data_fim"])
                    _mask = _dt <= _dt_fim
                    if _mask.any():
                        _pipeline_df = _pipeline_df[_mask].copy()
                        print(f"[gerar] Corte data_fim {params['data_fim']}: {len(_pipeline_df):,} candidatos restantes")
                except Exception as _e:
                    print(f"[WARN] data_fim inválida: {_e}")

        # Análise interna de qualidade (nunca vai para o Excel)
        qualidade = analisar_qualidade(_pipeline_df)

        # ── Definir diretório de saída ─────────────────────────────────────────
        def slug(t):
            t = t.lower().strip()
            t = re.sub(r"[^\w\s-]", "", t)
            return re.sub(r"[\s_-]+", "-", t)

        client_slug  = slug(params["cliente_nome"])
        periodo_slug = slug(params["periodo"] or "relatorio")
        out_dir = REPORTS_DIR / client_slug / periodo_slug
        out_dir.mkdir(parents=True, exist_ok=True)

        # ── JSON ──────────────────────────────────────────────────────────────
        # relatorio não contém "_df" (retornado separadamente pelo pipeline)
        rel_json = dict(relatorio)
        with open(out_dir / "data.json", "w", encoding="utf-8") as f:
            json.dump(rel_json, f, ensure_ascii=False, indent=2, default=str)

        # ── Dashboard compartilhável (persiste além do relatório normal) ──────
        share_id    = uuid.uuid4().hex[:16]
        share_url   = f"/d/{share_id}"
        share_xlsx  = None  # preenchido após gerar Excel

        # ── HTML Dashboard — injeta share_id para botão Excel persistente ─────
        template_path = Path(__file__).parent / "templates" / "dashboard.html"
        template = template_path.read_text(encoding="utf-8")
        rel_json["_share_id"] = share_id          # disponível em DATA._share_id no JS
        report_json = json.dumps(rel_json, ensure_ascii=False, default=str)
        html = template
        html = html.replace("{{CLIENTE}}", relatorio["meta"]["cliente"])
        html = html.replace("{{PERIODO}}", relatorio["meta"]["periodo"])
        html = html.replace("{{REPORT_JSON}}", report_json)
        # Injeta histórico do cliente (relatórios anteriores com snapshot)
        _hist_tmp = _get_cliente_historico(relatorio["meta"]["cliente"], share_id)
        html = html.replace('"__HISTORICO_JSON__"', json.dumps(_hist_tmp, ensure_ascii=False, default=str))
        html_path = out_dir / "index.html"
        html_path.write_text(html, encoding="utf-8")

        share_path  = DASHBOARDS_DIR / f"{share_id}.html"
        shutil.copy(str(html_path), str(share_path))

        # ── Salva dados analíticos para o chat IA (sem PII, LGPD compliant) ────
        # Exclui qualquer campo com dados individuais; mantém apenas agregados.
        _PII_KEYS = {"_df", "emails", "cpfs", "phones", "nomes", "candidatos"}
        def _strip_pii(obj, depth=0):
            if depth > 6: return obj
            if isinstance(obj, dict):
                return {k: _strip_pii(v, depth+1) for k, v in obj.items()
                        if k not in _PII_KEYS}
            if isinstance(obj, list):
                return [_strip_pii(i, depth+1) for i in obj]
            return obj
        try:
            analytics_data = _strip_pii(rel_json)
            analytics_data["_share_id"] = share_id
            analytics_path = DASHBOARDS_DIR / f"{share_id}_data.json"
            analytics_path.write_text(
                json.dumps(analytics_data, ensure_ascii=False, default=str),
                encoding="utf-8"
            )
        except Exception as _e:
            print(f"[WARN] Não foi possível salvar analytics data: {_e}", flush=True)

        # ── Segmentação — detectar ANTES do gerar_excel (df será consumido lá) ──
        tipo     = request.form.get("tipo_relatorio", "consolidado")
        dimensao = request.form.get("dimensao", "").strip()
        segmentacao_dims = []
        if tipo in ("segmentado", "ambos") and _pipeline_df is not None:
            try:
                dims = detect_dimensions(_pipeline_df)
                _DIM_LABELS = {
                    "filial": "Filial", "area": "Área", "recrutador": "Recrutador",
                    "cargo": "Cargo", "periodo": "Período",
                    "unidade": "Unidade de Negócio", "status": "Status",
                }
                if dimensao and dimensao in dims:
                    chosen = {dimensao: dims[dimensao]}
                elif dimensao and dims:
                    first_key = list(dims.keys())[0]
                    print(f"   ⚠️  Dimensão '{dimensao}' não detectada — usando '{first_key}'")
                    chosen = {first_key: dims[first_key]}
                elif not dimensao and dims:
                    chosen = dims
                else:
                    chosen = {}
                for dim_key, dim_info in chosen.items():
                    segmentacao_dims.append({
                        "col":   dim_info["col"],
                        "label": _DIM_LABELS.get(dim_key, dim_key.title()),
                    })
            except Exception as e:
                print(f"[WARN] Dim detection error: {e}", flush=True)

        # ── Excel ─────────────────────────────────────────────────────────────
        xlsx_path = str(out_dir / "relatorio.xlsx")
        # Apaga Excel antigo para evitar servir cache desatualizado
        try:
            import os as _os
            if _os.path.exists(xlsx_path):
                _os.remove(xlsx_path)
        except Exception:
            pass

        try:
            _roi = relatorio.get("roi", {})
            params_xls = dict(params)
            params_xls.update({
                "n_meses":  _roi.get("n_meses", 1),
                "saving":   _roi.get("savings"),
                "roi":      _roi.get("roi"),
                "total_ei": _roi.get("total_entrevistas_ia"),
            })
            # Passa df explicitamente via "_df" no relatorio para excel_gen (compatibilidade)
            relatorio_xls = dict(relatorio)
            relatorio_xls["_df"] = _pipeline_df
            _gerar_excel_novo(
                relatorio        = relatorio_xls,
                params           = params_xls,
                output_path      = xlsx_path,
                segmentacao_dims = segmentacao_dims,
            )
            del relatorio_xls
        except Exception as e:
            import traceback as _tb
            _excel_tb = _tb.format_exc()
            xlsx_path = None
            print(f"[Excel ERROR]\n{_excel_tb}", flush=True)
        else:
            _excel_tb = None
            # Copia xlsx para DASHBOARDS_DIR para persistir 30 dias junto com o dashboard
            if xlsx_path and _os.path.exists(xlsx_path):
                share_xlsx = DASHBOARDS_DIR / f"{share_id}.xlsx"
                shutil.copy(xlsx_path, str(share_xlsx))

        # Libera o DataFrame da memória — não é mais necessário após Excel gerado
        del _pipeline_df
        gc.collect()

        # Limpa uploads da sessão — relatório já está em disco, arquivos não são mais necessários
        _cleanup_dir(tmp_dir, delay=2)

        # ── Resposta ──────────────────────────────────────────────────────────
        kpis = relatorio.get("kpis") or {}
        roi  = relatorio.get("roi")  or {}
        # kpis pode ser {"_unavailable": ...} quando cenário 3 sem comparativo
        if isinstance(kpis, dict) and "_unavailable" in kpis:
            com = {}
        else:
            com = kpis.get("Com DigAI", {})

        saving = roi.get("savings", 0) or 0
        if saving >= 1_000_000:
            saving_fmt = f"{saving/1_000_000:.1f}M"
        elif saving >= 1_000:
            saving_fmt = f"{saving/1_000:.0f}k"
        else:
            saving_fmt = f"{saving:.0f}"

        xlsx_url_resp = f"/d/{share_id}.xlsx" if share_xlsx else None
        # Registra link gerado para recuperação futura
        _senha_plain, _senha_hash = _gerar_senha_dashboard()
        _registry_add(
            share_id  = share_id,
            cliente   = relatorio["meta"]["cliente"],
            periodo   = relatorio["meta"]["periodo"],
            xlsx_url  = xlsx_url_resp,
            snapshot  = _build_snapshot(relatorio),
            pwd_hash  = _senha_hash,
        )

        resp = {
            "ok":                  True,
            "html_url":            share_url,   # link persistente 30 dias (DASHBOARDS_DIR)
            "xlsx_url":            xlsx_url_resp,
            "share_url":           share_url,
            "kpis_com_contratados": com.get("contratados", 0),
            "roi_x":               int(roi.get("roi", 0)),
            "saving_fmt":          saving_fmt,
            "sla_com":             com.get("sla_media", "—"),
            "alertas_qualidade":   (relatorio.get("alertas_qualidade") or []) + (qualidade.get("alertas") or []),
            "desalinhados":        qualidade.get("desalinhados", []),
            "senha_dashboard":     _senha_plain,
        }
        if _excel_tb:
            resp["excel_error"] = _excel_tb
        return jsonify(resp)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[ERROR /gerar] session={session_id}\n{tb}", flush=True)
        # Retorna mensagem genérica — traceback não deve vazar para o cliente
        return jsonify({"error": str(e)}), 500


@app.route("/diagnostico", methods=["POST"])
def diagnostico():
    """
    Endpoint de diagnóstico: inspeciona os arquivos enviados e retorna
    colunas detectadas, amostras de emails e contagem de matches.
    Não gera relatório — só mostra o que o engine está enxergando.
    """
    import traceback

    # Resolver sessão (mesma lógica do gerar())
    sid = request.form.get("session_id", "").strip()
    if sid and re.match(r'^[0-9a-f]{8}$', sid):
        session_id = sid
        tmp_dir = UPLOAD_DIR / session_id
        if not tmp_dir.exists():
            return jsonify({"error": "Sessão não encontrada."}), 400

        def _list_d(field):
            return sorted([
                str(p) for p in tmp_dir.glob(f"{field}_*")
                if "_merged" not in p.name
            ]) or None

        funnel_paths_d = _list_d("funnel")
        cand_paths_d   = _list_d("candidatura")
        digai_paths_d  = _list_d("digai")
    else:
        session_id = uuid.uuid4().hex[:8]
        tmp_dir = UPLOAD_DIR / session_id
        tmp_dir.mkdir(parents=True)

        def _save_diag(field):
            files, paths = request.files.getlist(field), []
            for i, f in enumerate(files):
                if f and f.filename:
                    ext = Path(f.filename).suffix or ".csv"
                    p = tmp_dir / f"{field}_{i}{ext}"; f.save(str(p)); paths.append(str(p))
            return paths or None

        funnel_paths_d = _save_diag("funnel")
        cand_paths_d   = _save_diag("candidatura")
        digai_paths_d  = _save_diag("digai")

    try:
        funnel_path      = merge_upload_files(funnel_paths_d, "funnel",      tmp_dir)
        candidatura_path = merge_upload_files(cand_paths_d,   "candidatura", tmp_dir)
        digai_path       = merge_upload_files(digai_paths_d,  "digai",       tmp_dir)

        from engine.ingestion import load_pipeline, load_gupy_candidatura, load_digai_base
        from engine.ingestion import normalize_email

        result = {}

        def _safe(v):
            """Converte tipos numpy/pandas para tipos Python nativos (JSON-safe)."""
            import numpy as np
            if isinstance(v, (np.bool_,)):
                return bool(v)
            if isinstance(v, (np.integer,)):
                return int(v)
            if isinstance(v, (np.floating,)):
                return float(v)
            return v

        if funnel_path:
            _f_result = load_pipeline(funnel_path)
            df_f = _f_result.df  # IngestionResult → DataFrame
            result["funnel"] = {
                "rows": int(len(df_f)),
                "columns": list(df_f.columns[:40]),
                "email_found": bool("email" in df_f.columns),
                "emails_com_valor": int(df_f["email"].ne("").sum()),
                "sample_emails": df_f[df_f["email"].ne("")]["email"].head(5).tolist(),
                "stages_detected": len(_f_result.stage_cols),
                "ei_stage_col": _f_result.ei_stage_col,
                "status_counts": {str(k): int(v) for k, v in
                    df_f["status"].value_counts().head(10).items()} if "status" in df_f.columns else {},
            }
        else:
            df_f = None

        df_c = None
        if candidatura_path:
            _c_result = load_gupy_candidatura(candidatura_path)
            df_c = _c_result.df  # CandidaturaResult → DataFrame
            n_contratados = 0
            if "status_cand" in df_c.columns:
                n_contratados = int((df_c["status_cand"] == "Contratado").sum())
            elif "data_contratacao_cand" in df_c.columns:
                n_contratados = int(df_c["data_contratacao_cand"].notna().sum())
            result["candidatura"] = {
                "rows": int(len(df_c)),
                "columns": list(df_c.columns[:40]),
                "email_found": bool("email" in df_c.columns),
                "emails_com_valor": int(df_c["email"].ne("").sum()) if "email" in df_c.columns else 0,
                "sample_emails": df_c[df_c["email"].ne("")]["email"].head(5).tolist() if "email" in df_c.columns else [],
                "contratados": n_contratados,
                "is_anchor": _c_result.is_contratados,
            }

        df_d = None
        if digai_path:
            _d_result = load_digai_base(digai_path)
            df_d = _d_result.df  # DigAIResult → DataFrame
            result["digai"] = {
                "rows": int(len(df_d)),
                "total_pre_dedup": _d_result.total,
                "columns": list(df_d.columns[:40]),
                "email_found": bool("email" in df_d.columns),
                "emails_com_valor": int(df_d["email"].ne("").sum()),
                "cpf_found": bool("cpf" in df_d.columns),
                "cpfs_com_valor": int(df_d["cpf"].ne("").sum()) if "cpf" in df_d.columns else 0,
                "sample_emails": df_d[df_d["email"].ne("")]["email"].head(5).tolist(),
                "data_ei_found": bool(df_d["data_ei"].notna().any()),
                "data_ei_nao_nulos": int(df_d["data_ei"].notna().sum()),
            }

        # ── Cruzamento de emails ──────────────────────────────────────────────
        if df_d is not None and "email" in df_d.columns:
            emails_digai = set(df_d[df_d["email"].ne("")]["email"])

            # Decide fonte: funnel tem emails? senão usa candidatura
            fonte = "funnel"
            df_fonte = df_f if df_f is not None else None
            funnel_emails_count = int(df_f["email"].ne("").sum()) if (df_f is not None and "email" in df_f.columns) else 0
            if funnel_emails_count == 0 and df_c is not None and "email" in df_c.columns:
                df_fonte = df_c
                fonte = "candidatura"

            if df_fonte is not None and "email" in df_fonte.columns:
                emails_fonte = set(df_fonte[df_fonte["email"].ne("")]["email"])
                matches = emails_fonte & emails_digai

                match_info = {
                    "fonte": fonte,
                    "emails_unicos_fonte": int(len(emails_fonte)),
                    "emails_unicos_digai":  int(len(emails_digai)),
                    "matches_email":        int(len(matches)),
                    "sample_matches":       list(matches)[:5],
                }

                # Contratados que aparecem na base DigAI
                if df_c is not None and "email" in df_c.columns:
                    # Pega emails dos contratados
                    if "status_cand" in df_c.columns:
                        hired_mask = df_c["status_cand"] == "Contratado"
                    elif "data_contratacao_cand" in df_c.columns:
                        hired_mask = df_c["data_contratacao_cand"].notna()
                    else:
                        hired_mask = df_c["email"].ne("")  # fallback: todos

                    emails_hired = set(df_c[hired_mask & df_c["email"].ne("")]["email"])
                    hired_in_digai = emails_hired & emails_digai
                    match_info["hired_total"]       = int(len(emails_hired))
                    match_info["hired_in_digai"]    = int(len(hired_in_digai))
                    match_info["sample_hired_digai"] = list(hired_in_digai)[:5]

                result["match"] = match_info

        # Normaliza todo o dict para tipos JSON-safe, contornando o provider Flask 2.x
        import json as _json
        import numpy as _np
        from flask import Response as _Response

        def _json_safe(obj):
            if isinstance(obj, dict):
                return {k: _json_safe(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_json_safe(v) for v in obj]
            if isinstance(obj, bool):        # bool antes de int (bool é subclasse de int)
                return int(obj)
            if isinstance(obj, _np.bool_):
                return int(obj)
            if isinstance(obj, _np.integer):
                return int(obj)
            if isinstance(obj, _np.floating):
                return float(obj)
            if isinstance(obj, _np.ndarray):
                return [_json_safe(v) for v in obj.tolist()]
            if isinstance(obj, set):
                return list(obj)
            return obj

        return _Response(
            _json.dumps(_json_safe(result), ensure_ascii=False),
            mimetype="application/json"
        )

    except Exception:
        tb = traceback.format_exc()
        print(f"[ERROR /diagnostico] {tb}", flush=True)
        return jsonify({"error": tb}), 500


@app.route("/relatorio/<cliente>/<periodo>/")
@app.route("/relatorio/<cliente>/<periodo>/index.html")
def serve_report(cliente, periodo):
    path = REPORTS_DIR / cliente / periodo / "index.html"
    if path.exists():
        return path.read_text(encoding="utf-8"), 200, {"Content-Type": "text/html"}
    return "Relatório não encontrado.", 404


@app.route("/relatorio/<cliente>/<periodo>/relatorio.xlsx")
def serve_xlsx(cliente, periodo):
    path = REPORTS_DIR / cliente / periodo / "relatorio.xlsx"
    if path.exists():
        report_dir = REPORTS_DIR / cliente / periodo
        _cleanup_dir(report_dir, delay=30)  # limpa 30s após envio
        return send_file(str(path), as_attachment=True, download_name="relatorio.xlsx")
    return "Excel não encontrado.", 404


@app.route("/relatorio/<cliente>/<periodo>/relatorio_segmentado.xlsx")
def serve_xlsx_seg(cliente, periodo):
    path = REPORTS_DIR / cliente / periodo / "relatorio_segmentado.xlsx"
    if path.exists():
        report_dir = REPORTS_DIR / cliente / periodo
        _cleanup_dir(report_dir, delay=30)
        return send_file(str(path), as_attachment=True, download_name="relatorio_segmentado.xlsx")
    return "Excel segmentado não encontrado.", 404


def _render_password_page(share_id: str, erro: bool = False) -> str:
    """Página de autenticação por senha para dashboards protegidos."""
    erro_html = '<p style="color:#f87171;font-size:0.8rem;margin-top:0.75rem">Senha incorreta. Tente novamente.</p>' if erro else ''
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>DigAI — Acesso Protegido</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#0d1b2e;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         display:flex;align-items:center;justify-content:center;min-height:100vh}}
    .box{{background:#1e293b;border:1px solid #334155;border-radius:12px;
          padding:2.5rem 2rem;width:100%;max-width:380px;text-align:center}}
    .logo{{font-size:1.4rem;font-weight:800;color:#00caf3;letter-spacing:-0.5px;margin-bottom:0.25rem}}
    .sub{{font-size:0.8rem;color:#64748b;margin-bottom:2rem}}
    label{{display:block;text-align:left;font-size:0.75rem;color:#94a3b8;
           font-weight:600;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.4rem}}
    input{{width:100%;padding:0.7rem 1rem;background:#0d1b2e;border:1px solid #334155;
           border-radius:8px;color:#f1f5f9;font-size:1rem;letter-spacing:0.15em;
           text-align:center;outline:none;transition:border-color 0.15s}}
    input:focus{{border-color:#00caf3}}
    button{{margin-top:1rem;width:100%;padding:0.75rem;background:#2563eb;
            border:none;border-radius:8px;color:#fff;font-weight:700;
            font-size:0.9rem;cursor:pointer;transition:background 0.15s}}
    button:hover{{background:#1d4ed8}}
  </style>
</head>
<body>
  <div class="box">
    <div class="logo">DigAI</div>
    <div class="sub">Relatório protegido — insira a senha de acesso</div>
    <form method="POST" action="/d/{share_id}/auth">
      <label>Senha</label>
      <input type="password" name="senha" autofocus autocomplete="off" placeholder="&bull;&bull;&bull;&bull;&bull;&bull;&bull;&bull;"/>
      {erro_html}
      <button type="submit">Acessar</button>
    </form>
  </div>
</body>
</html>"""


@app.route("/d/<share_id>/auth", methods=["POST"])
def dashboard_auth(share_id):
    """Valida senha do dashboard e cria sessão autenticada."""
    from werkzeug.security import check_password_hash
    if not re.match(r'^[0-9a-f]{16}$', share_id):
        return "Link inválido.", 404
    senha = request.form.get("senha", "").strip()
    with _REGISTRY_LOCK:
        entries = _registry_load()
    entry = next((e for e in entries if e.get("share_id") == share_id), None)
    if not entry:
        return "Dashboard não encontrado.", 404
    if check_password_hash(entry.get("pwd_hash", ""), senha):
        session.permanent = True
        session[f"auth_{share_id}"] = True
        return redirect(f"/d/{share_id}")
    return _render_password_page(share_id, erro=True), 200, {"Content-Type": "text/html"}


@app.route("/d/<share_id>")
def dashboard_share(share_id):
    """Serve dashboard HTML persistente pelo link compartilhável.
    Se existir _data.json, re-renderiza com o template atual para garantir
    que todas as features do template mais recente estejam presentes.
    """
    if not re.match(r'^[0-9a-f]{16}$', share_id):
        return "Link inválido.", 404
    path = DASHBOARDS_DIR / f"{share_id}.html"
    if not path.exists():
        return "Dashboard não encontrado ou expirado (links são válidos por 30 dias).", 404

    # Guard de autenticação — só exige senha se pwd_hash existe no registry
    if not session.get(f"auth_{share_id}"):
        with _REGISTRY_LOCK:
            entries = _registry_load()
        entry = next((e for e in entries if e.get("share_id") == share_id), None)
        pwd_hash = entry.get("pwd_hash", "") if entry else ""
        if pwd_hash:
            return _render_password_page(share_id), 200, {"Content-Type": "text/html"}

    # Re-renderiza com template atual se _data.json disponível
    data_path = DASHBOARDS_DIR / f"{share_id}_data.json"
    if data_path.exists():
        try:
            rel_json = json.loads(data_path.read_text(encoding="utf-8"))
            template_path = Path(__file__).parent / "templates" / "dashboard.html"
            template = template_path.read_text(encoding="utf-8")
            meta = rel_json.get("meta", {})
            report_json = json.dumps(rel_json, ensure_ascii=False, default=str)
            html = template
            html = html.replace("{{CLIENTE}}", meta.get("cliente", ""))
            html = html.replace("{{PERIODO}}", meta.get("periodo", ""))
            html = html.replace("{{REPORT_JSON}}", report_json)
            _hist = _get_cliente_historico(meta.get("cliente", ""), share_id)
            html = html.replace('"__HISTORICO_JSON__"', json.dumps(_hist, ensure_ascii=False, default=str))
            return html, 200, {"Content-Type": "text/html"}
        except Exception:
            pass  # fallback para HTML estático

    return path.read_text(encoding="utf-8"), 200, {"Content-Type": "text/html"}


@app.route("/d/<share_id>.xlsx")
def dashboard_share_xlsx(share_id):
    """
    Serve o Excel gerado. O arquivo é mantido por 30 dias (mesmo ciclo do HTML).
    Múltiplos downloads do mesmo link são permitidos.
    """
    if not re.match(r'^[0-9a-f]{16}$', share_id):
        return "Link inválido.", 404
    path = DASHBOARDS_DIR / f"{share_id}.xlsx"
    if not path.exists():
        return "Excel não encontrado ou expirado (links são válidos por 30 dias).", 404
    return send_file(
        str(path),
        as_attachment=True,
        download_name="relatorio.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/static/<path:filename>")
def serve_static(filename):
    """Serve arquivos estáticos (GeoJSON, imagens, etc.)."""
    static_dir = Path(__file__).parent / "static"
    path = static_dir / filename
    if not path.exists() or not path.resolve().is_relative_to(static_dir.resolve()):
        return "Arquivo não encontrado.", 404
    mime = {
        ".geojson": "application/json",
        ".json":    "application/json",
        ".svg":     "image/svg+xml",
        ".png":     "image/png",
        ".js":      "application/javascript",
        ".css":     "text/css",
    }.get(path.suffix, "application/octet-stream")
    return path.read_bytes(), 200, {"Content-Type": mime}


@app.route("/chat", methods=["POST"])
def chat_with_data():
    """
    Chat IA com os dados do relatorio.
    LGPD: nunca expoe dados pessoais individuais.
    Acessa _data.json salvo junto ao dashboard para contexto completo.
    """
    import traceback as _tb
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({"error": "SDK anthropic nao instalado."}), 500

    try:
        body         = request.get_json(force=True) or {}
        user_message = (body.get("message") or "").strip()
        share_id     = (body.get("share_id") or "").strip()
        history      = body.get("history") or []

        if not user_message:
            return jsonify({"error": "Mensagem vazia."}), 400

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY nao configurada no servidor."}), 500

        # Carrega dados analiticos do relatorio (sem PII)
        data = {}
        if share_id and re.match(r'^[0-9a-f]{16}$', share_id):
            data_path = DASHBOARDS_DIR / f"{share_id}_data.json"
            if data_path.exists():
                try:
                    data = json.loads(data_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

        # Helpers de formatacao
        def _fp(v):
            try: return f"{float(v)*100:.1f}%" if float(v) <= 1 else f"{float(v):.1f}%"
            except: return str(v)
        def _fn(v):
            try: return f"{int(v):,}"
            except: return str(v)
        def _fb(v):
            try: return f"R$ {float(v):,.0f}"
            except: return str(v)

        sections = []
        meta = data.get("meta", {})
        if meta:
            sections.append(f"CLIENTE: {meta.get('cliente','?')} | PERIODO: {meta.get('periodo','?')} | Gerado: {meta.get('gerado_em','?')}")

        kpis = data.get("kpis", {})
        com  = kpis.get("Com DigAI", {})
        sem  = kpis.get("Sem DigAI", {})
        dlt  = kpis.get("delta", {})
        if com or sem:
            sections.append("\n### KPIs PRINCIPAIS")
            sections.append(f"Total candidatos: Com DigAI={_fn(com.get('total',0))} | Sem DigAI={_fn(sem.get('total',0))}")
            sections.append(f"Contratados: Com DigAI={_fn(com.get('contratados',0))} | Sem DigAI={_fn(sem.get('contratados',0))}")
            sections.append(f"Taxa de contratacao: Com DigAI={_fp(com.get('taxa_contratacao',0))} | Sem DigAI={_fp(sem.get('taxa_contratacao',0))} | delta={dlt.get('contratacoes_pct','?')}")
            sections.append(f"Assertividade IA: {com.get('assertividade','?')} entrevistas por contratacao")
            sections.append(f"SLA medio: Com DigAI={com.get('sla_media','?')} dias | Sem DigAI={sem.get('sla_media','?')} dias")
            sections.append(f"Adesao DigAI: {_fp(com.get('adesao',0))}")

        roi = data.get("roi", {})
        if roi:
            sections.append("\n### ROI E SAVINGS")
            sections.append(f"Savings DigAI: {_fb(roi.get('savings',0))}")
            sections.append(f"ROI: {roi.get('roi','?')}x sobre o investimento")
            sections.append(f"Entrevistas IA realizadas: {_fn(roi.get('total_entrevistas_ia',0))}")
            sections.append(f"Custo por EI IA: {_fb(roi.get('custo_por_entrevista_ia',0))} vs TA: {_fb(roi.get('custo_por_entrevista_ta',0))}")
            sections.append(f"Economia por entrevista: {_fb(roi.get('economia_por_entrevista',0))}")
            sections.append(f"Investimento total: {_fb(roi.get('investimento_total',0))} | Meses: {roi.get('n_meses','?')}")

        funil = data.get("funil_din") or data.get("funil", [])
        if funil:
            sections.append("\n### FUNIL DE CONVERSAO")
            for e in funil:
                sections.append(f"  {e.get('etapa','?')}: Com={_fn(e.get('com_digai',0))} ({e.get('pct_com','?')}%) | Sem={_fn(e.get('sem_digai',0))} ({e.get('pct_sem','?')}%) | drop={e.get('dropoff_com','?')}%")

        tempos = data.get("tempos_din") or data.get("tempos", [])
        if tempos:
            sections.append("\n### TEMPO MEDIO POR ETAPA (dias)")
            for t in tempos:
                sections.append(f"  {t.get('etapa','?')}: Com={t.get('com_digai','?')}d | Sem={t.get('sem_digai','?')}d | {t.get('impacto','?')}")

        status = data.get("status", [])
        if status:
            sections.append("\n### STATUS DOS CANDIDATOS")
            for s in status:
                sections.append(f"  {s.get('status','?')}: Com={_fn(s.get('com_digai',0))} ({s.get('pct_com','?')}%) | Sem={_fn(s.get('sem_digai',0))} ({s.get('pct_sem','?')}%)")

        origem = data.get("origem_candidatos", [])
        if origem:
            validos = [r for r in origem if r.get("uf") not in ("Sem DDD","Sem Estado","Outro","")]
            sections.append(f"\n### DISTRIBUICAO GEOGRAFICA (top {min(10,len(validos))} estados)")
            for r in validos[:10]:
                sections.append(f"  {r.get('uf','?')}: {_fn(r.get('total',0))} candidatos ({r.get('pct','?')}%)")

        vagas = data.get("mapa_vagas", [])
        if vagas:
            sections.append(f"\n### MAPA DE VAGAS (top {min(10,len(vagas))})")
            for v in vagas[:10]:
                sections.append(f"  {v.get('vaga','?')}: {_fn(v.get('total',0))} candidatos | contratados={_fn(v.get('contratados',0))} | adesao={v.get('adesao','?')}% | SLA={v.get('sla_medio','?')}d")

        perfil = data.get("perfil_aprovados", {})
        if perfil:
            genero = perfil.get("genero", {})
            if genero:
                sections.append("\n### PERFIL DOS CONTRATADOS — GENERO")
                sections.append(f"  Feminino: {_fn(genero.get('Feminino',0))} ({genero.get('pct_feminino',0)}%) | Masculino: {_fn(genero.get('Masculino',0))} ({genero.get('pct_masculino',0)}%)")
            score = perfil.get("score", {})
            if score and score.get("media"):
                sections.append(f"  Score IA medio contratados: {score.get('media','?')} (mediana: {score.get('mediana','?')})")

        periodo_comp = data.get("periodo_comparativo", [])
        # periodo_comparativo pode ser lista de meses ou dict com chave "meses"
        if isinstance(periodo_comp, dict):
            periodo_comp = periodo_comp.get("meses", [])
        if periodo_comp:
            sections.append("\n### EVOLUCAO MENSAL (ultimos 6 meses)")
            for m in periodo_comp[-6:]:
                if not isinstance(m, dict):
                    continue
                total = m.get('total', m.get('total_com', 0) + m.get('total_sem', 0))
                sections.append(f"  {m.get('periodo','?')}: {_fn(total)} cands | {_fn(m.get('contratados',0))} contratados | Com DigAI={_fn(m.get('com_digai', m.get('total_com',0)))}")

        insights = data.get("insights", {})
        if insights:
            sections.append("\n### INSIGHTS")
            sections.append(f"Veredicto: {insights.get('veredicto','?')}")
            for p in (insights.get("pontos_positivos") or []):
                sections.append(f"  + {p}")
            for p in (insights.get("pontos_atencao") or []):
                sections.append(f"  ! {p}")

        # Histórico de períodos anteriores do mesmo cliente
        _hist_chat = _get_cliente_historico(meta.get("cliente", ""), share_id)
        if _hist_chat:
            sections.append("\n### HISTORICO DO CLIENTE (relatorios anteriores — do mais antigo ao mais recente)")
            for h in _hist_chat[-6:]:
                _savings = h.get("savings", 0)
                try:
                    _savings_fmt = f"R${float(_savings):,.0f}"
                except Exception:
                    _savings_fmt = str(_savings)
                sections.append(
                    f"  {h.get('periodo','?')}: "
                    f"contratados_com={h.get('contratados_com','?')} | "
                    f"taxa_com={h.get('taxa_com','?')}% | "
                    f"savings={_savings_fmt} | "
                    f"ROI={h.get('roi','?')}x | "
                    f"adesao={h.get('adesao_pct','?')}%"
                )

        ctx_data = "\n".join(sections) if sections else "Dados nao disponiveis para este relatorio."
        cliente_nome = meta.get("cliente", "Cliente")
        periodo_nome = meta.get("periodo", "Periodo")

        system_prompt = f"""Voce e a IA Analitica do DigAI — uma Chief Analytics Officer virtual especializada em People Analytics e recrutamento inteligente por IA.

Seu papel e conversar com executivos (C-level, CHROs, VPs de RH e Operacoes) sobre os dados de performance de recrutamento da empresa {cliente_nome}, traduzindo numeros em decisoes estrategicas.

## IDENTIDADE E POSTURA
- Comunique-se como uma consultora senior de dados: assertiva, objetiva, orientada a impacto de negocio
- Use linguagem executiva: foque em eficiencia operacional, custo, ROI, velocidade e qualidade de contratacao
- Cite os dados exatos do relatorio para embasar cada afirmacao — nunca faca suposicoes
- Quando identificar oportunidade ou risco, aponte a acao recomendada de forma clara e direta
- CEOs valorizam clareza: seja concisa, sem introducoes longas nem conclusoes redundantes
- Use **negrito** para termos-chave e marcadores com "—" para listas
- Responda sempre em portugues brasileiro

## FORMATACAO — REGRAS OBRIGATORIAS
- NUNCA use emojis. Nenhum. Zero. Absolutamente proibido
- NUNCA use exclamacoes ou linguagem entusiastica ("Otimo!", "Excelente!", etc)
- Estruture com marcadores limpos ("—") e **negrito** para destaques
- Cabecalhos de secao em negrito se necessario, sem simbolos decorativos
- Tom: direto, tecnico, estrategico — como um briefing executivo escrito

## REGRAS LGPD — ABSOLUTAS E INEGOCIAVEIS
- NUNCA informe nomes, emails, CPF, telefone ou qualquer dado que identifique um candidato individualmente
- Se perguntado sobre dados pessoais (ex: "quem foi contratado?", "qual o nome de X?"), recuse com objetividade: informe que por conformidade com a LGPD esses dados nao estao disponiveis neste canal, e ofeca o agregado correspondente (ex: total de contratados, distribuicao por vaga)
- Dados agregados, medias, percentuais, distribuicoes: sempre permitidos
- Dados de vagas, areas, filiais e periodos: permitidos (dados organizacionais)

## ESCOPO
- Responda APENAS sobre os dados do relatorio abaixo
- Se perguntado sobre algo fora do escopo, diga claramente e sugira o que pode responder com os dados disponiveis
- Nao invente ou estime dados que nao estao no relatorio

## DADOS DO RELATORIO — {cliente_nome} | {periodo_nome}
{ctx_data}
"""

        # Historico de conversa (max 10 turnos)
        messages = []
        for h in (history or [])[-10:]:
            role    = h.get("role", "")
            content = h.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": user_message})

        client = _anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            system=system_prompt,
            messages=messages,
        )
        answer = response.content[0].text if response.content else "Sem resposta."
        return jsonify({"answer": answer})

    except Exception:
        print(f"[ERROR /chat] {_tb.format_exc()}", flush=True)
        return jsonify({"error": "Erro ao processar chat."}), 500


@app.route("/links")
def listar_links():
    """Lista todos os dashboards gerados — útil para recuperar links após fechar a janela."""
    with _REGISTRY_LOCK:
        entries = _registry_load()
    if not entries:
        return "<p>Nenhum dashboard gerado ainda.</p>", 200, {"Content-Type": "text/html"}

    rows = ""
    for e in reversed(entries):  # mais recentes primeiro
        url     = e.get("url", "")
        cliente = e.get("cliente", "?")
        periodo = e.get("periodo", "?")
        criado  = e.get("criado_em", "?")
        xlsx    = e.get("xlsx_url")
        xlsx_cell = f'<a href="{xlsx}">⬇ Excel</a>' if xlsx else '<span style="color:#888">já baixado</span>'
        rows += (
            f"<tr>"
            f"<td>{criado}</td>"
            f"<td><strong>{cliente}</strong></td>"
            f"<td>{periodo}</td>"
            f"<td><a href='{url}' target='_blank'>🔗 Dashboard</a></td>"
            f"<td>{xlsx_cell}</td>"
            f"</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <title>Links Gerados — DigAI</title>
  <style>
    body{{font-family:sans-serif;background:#06101f;color:#e0e8f0;padding:2rem}}
    h1{{color:#00caf3;margin-bottom:1.5rem}}
    table{{width:100%;border-collapse:collapse}}
    th{{background:#1b4fd8;color:#fff;padding:0.6rem 1rem;text-align:left}}
    td{{padding:0.5rem 1rem;border-bottom:1px solid rgba(255,255,255,0.08)}}
    tr:hover td{{background:rgba(0,202,243,0.06)}}
    a{{color:#00caf3}}
  </style>
</head>
<body>
  <h1>📋 Dashboards gerados ({len(entries)} total)</h1>
  <p style="color:#8ba3c4;margin-bottom:1rem">
    Os links são válidos por 30 dias. O Excel é removido após o primeiro download.
  </p>
  <table>
    <thead><tr>
      <th>Gerado em</th><th>Cliente</th><th>Período</th>
      <th>Dashboard</th><th>Excel</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>"""
    return html, 200, {"Content-Type": "text/html"}


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  DigAI Reports Engine — Interface Web")
    print("="*55)
    port = int(os.environ.get("PORT", 5001))
    print(f"  Acesse: http://localhost:{port}")
    print("  Para encerrar: Ctrl+C")
    print("="*55 + "\n")
    app.run(host="0.0.0.0", port=port, debug=False)
