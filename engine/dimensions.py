"""
DigAI Reports Engine — Segmentação por Dimensões

Detecta automaticamente dimensões disponíveis na base (área, filial,
recrutador, período, unidade extraída do nome da vaga) e suporta
geração de relatórios por segmento.

Uso:
    from engine.dimensions import detect_dimensions, filter_by_segment
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any


# ─── Constantes ────────────────────────────────────────────────────────────────

# Colunas Gupy → nome da dimensão
DIRECT_DIMENSIONS = {
    "area":       ["Área da vaga", "Area da vaga", "Area", "Área"],
    "filial":     ["Filial", "Unidade", "Localidade"],
    "recrutador": ["Responsável pela ação", "Recrutador", "Responsavel", "Owner"],
    "cargo":      ["Cargo", "Função", "Funcao", "Posição", "Posicao"],
    "status":     ["Status na vaga"],
}

# Limites para dimensão válida (evita colunas únicas ou explodem)
MIN_UNIQUE = 2
MAX_UNIQUE = 50


# ─── Detecção de dimensões ────────────────────────────────────────────────────

def detect_dimensions(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Detecta as dimensões disponíveis para segmentação.

    Retorna dict{dim_key: {col, values, count}}, onde:
      - dim_key: chave amigável (ex: "area", "filial", "periodo")
      - col: nome da coluna no DataFrame
      - values: lista de valores únicos ordenados
      - count: número de valores únicos
    """
    dims = {}

    # 1. Dimensões diretas
    for dim_key, col_candidates in DIRECT_DIMENSIONS.items():
        for col in col_candidates:
            if col in df.columns:
                vals = df[col].dropna().astype(str).str.strip()
                vals = vals[vals != ""].unique().tolist()
                if MIN_UNIQUE <= len(vals) <= MAX_UNIQUE:
                    dims[dim_key] = {
                        "col": col,
                        "values": sorted(vals),
                        "count": len(vals),
                    }
                break

    # 2. Período — extraído da data de cadastro
    date_col = None
    for c in ("data_cadastro", "data_inscricao_funnel", "stage_1_entry"):
        if c in df.columns:
            date_col = c
            break
    if date_col:
        periodos = (
            pd.to_datetime(df[date_col], errors="coerce")
            .dropna()
            .dt.to_period("M")
            .astype(str)
            .unique()
            .tolist()
        )
        periodos = sorted([p for p in periodos if p != "NaT"])
        if len(periodos) >= 2:
            # Gera coluna auxiliar no df
            df["_periodo_mensal"] = (
                pd.to_datetime(df[date_col], errors="coerce")
                .dt.to_period("M")
                .astype(str)
            )
            dims["periodo"] = {
                "col": "_periodo_mensal",
                "values": periodos,
                "count": len(periodos),
            }

    # 3. Unidade — extraída do nome da vaga (padrão: "CODIGO UNIDADE - Cargo")
    vaga_col = None
    for c in ("vaga", "vaga_cand", "Nome da vaga", "vaga_digai"):
        if c in df.columns:
            vaga_col = c
            break
    if vaga_col:
        # Padrão: NNNN-NNNNNNN UNIDADE - Cargo  (ex: "0646-10919122 RET CAB DIGAI")
        # ou: "ATENTO ESCALADA - Especialista SAC"
        def extract_unidade(s):
            if pd.isna(s):
                return None
            s = str(s).strip()
            # Remove código numérico no início
            s = re.sub(r"^\d[\d\-]+\s+", "", s)
            # Tudo antes do primeiro " - "
            m = re.match(r"^(.+?)\s+-\s+.+", s)
            if m:
                return m.group(1).strip()
            # Se curto, retorna o nome todo
            if len(s) <= 40:
                return s
            return None

        df["_unidade_vaga"] = df[vaga_col].apply(extract_unidade)
        unidades = df["_unidade_vaga"].dropna().unique().tolist()
        unidades = [u for u in unidades if u]
        if MIN_UNIQUE <= len(unidades) <= MAX_UNIQUE:
            dims["unidade"] = {
                "col": "_unidade_vaga",
                "values": sorted(unidades),
                "count": len(unidades),
            }

    return dims


def print_dimensions(dims: Dict[str, Dict], client_name: str = "") -> None:
    """Exibe dimensões encontradas de forma amigável."""
    if not dims:
        print("   ℹ️  Nenhuma dimensão adicional detectada.")
        return

    print(f"\n{'─'*60}")
    print(f"  Dimensões disponíveis para segmentação:")
    print(f"{'─'*60}")
    keys = list(dims.keys())
    for i, (key, info) in enumerate(dims.items(), start=1):
        examples = ", ".join(str(v) for v in info["values"][:3])
        tail = f"... (+{info['count']-3})" if info['count'] > 3 else ""
        print(f"  [{i}] {key:<15} → {info['count']} valores  |  ex: {examples}{tail}")
    print(f"{'─'*60}\n")


# ─── Filtro por segmento ──────────────────────────────────────────────────────

def filter_by_segment(df: pd.DataFrame, dim_col: str, segment_value: str) -> pd.DataFrame:
    """Retorna subconjunto do df filtrado para um segmento específico."""
    if dim_col not in df.columns:
        raise ValueError(f"Coluna '{dim_col}' não encontrada no DataFrame.")
    filtered = df[df[dim_col].astype(str).str.strip() == str(segment_value).strip()].copy()
    filtered.attrs.update(df.attrs)  # preserva metadata (stage_cols, etc.)
    return filtered


# ─── Wizard de configuração (modo CLI) ───────────────────────────────────────

def run_config_wizard(df: pd.DataFrame, params: dict) -> dict:
    """
    Wizard interativo (CLI) para configuração do relatório.
    Retorna config dict com mode, dimension, segments, output_type.
    """
    dims = detect_dimensions(df)
    client_name = params.get("cliente_nome", "Cliente")

    print(f"\n  Arquivo carregado: {len(df):,} candidatos")

    if dims:
        print_dimensions(dims, client_name)
        print("  Como deseja o relatório?")
        print("  [1] Consolidado — visão geral (padrão)")
        print("  [2] Segmentado  — separar por dimensão")
        print("  [3] Ambos       — consolidado + segmentado")
        try:
            modo = input("\n  Digite o número [1]: ").strip() or "1"
        except (EOFError, KeyboardInterrupt):
            modo = "1"
    else:
        modo = "1"

    config = {
        "mode": {"1": "consolidado", "2": "segmentado", "3": "ambos"}.get(modo, "consolidado"),
        "dimension": None,
        "dim_col": None,
        "segments": None,
        "output_type": "single_file",
        "dims_available": dims,
    }

    if config["mode"] == "consolidado":
        return config

    # ── Qual dimensão? ─────────────────────────────────────────────────────────
    keys = list(dims.keys())
    print(f"\n  Por qual dimensão?")
    for i, key in enumerate(keys, start=1):
        info = dims[key]
        examples = ", ".join(str(v) for v in info["values"][:3])
        print(f"  [{i}] {key} ({examples}...)")
    print(f"  [{len(keys)+1}] Customizado")

    try:
        dim_choice = int(input(f"\n  Digite o número: ").strip()) - 1
    except (ValueError, EOFError, KeyboardInterrupt):
        dim_choice = 0

    if dim_choice < len(keys):
        chosen_key = keys[dim_choice]
        config["dimension"] = chosen_key
        config["dim_col"]   = dims[chosen_key]["col"]
        all_values = dims[chosen_key]["values"]
    else:
        col_name = input("  Nome da coluna: ").strip()
        config["dimension"] = col_name
        config["dim_col"]   = col_name
        all_values = df[col_name].dropna().unique().tolist() if col_name in df.columns else []

    # ── Quais segmentos? ───────────────────────────────────────────────────────
    print(f"\n  Valores em '{config['dimension']}':")
    for i, v in enumerate(all_values, start=1):
        cnt = len(df[df[config["dim_col"]].astype(str).str.strip() == str(v)])
        print(f"  [{i}] {str(v):<40} {cnt:,} candidatos")

    print("\n  [A] Todos  [S] Selecionar  [E] Excluir")
    try:
        sel = input("\n  Escolha [A]: ").strip().upper() or "A"
    except (EOFError, KeyboardInterrupt):
        sel = "A"

    if sel == "S":
        try:
            idxs = input("  Números (ex: 1,2,4): ").strip()
            config["segments"] = [all_values[int(i)-1] for i in idxs.split(",") if i.strip()]
        except Exception:
            config["segments"] = all_values
    elif sel == "E":
        try:
            idxs = input("  Números a excluir: ").strip()
            excluir = {all_values[int(i)-1] for i in idxs.split(",") if i.strip()}
            config["segments"] = [v for v in all_values if v not in excluir]
        except Exception:
            config["segments"] = all_values
    else:
        config["segments"] = all_values

    # ── Formato de saída (se muitos segmentos) ────────────────────────────────
    n_segs = len(config["segments"])
    if n_segs > 10:
        print(f"\n  {n_segs} segmentos encontrados (>10).")
        print("  [1] 1 arquivo com abas separadas")
        print("  [2] 1 arquivo por segmento (batch)")
        try:
            fmt = input("  Formato [1]: ").strip() or "1"
        except (EOFError, KeyboardInterrupt):
            fmt = "1"
        config["output_type"] = "batch" if fmt == "2" else "single_file"

    # ── Confirmação ────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  Configuração:")
    print(f"  Modo:      {config['mode']}")
    print(f"  Dimensão:  {config['dimension']}")
    segs_str = ", ".join(str(s) for s in (config["segments"] or [])[:5])
    if config["segments"] and len(config["segments"]) > 5:
        segs_str += f" ... (+{len(config['segments'])-5})"
    print(f"  Segmentos: {segs_str or 'todos'}")
    print(f"  Saída:     {config['output_type']}")
    print(f"{'─'*60}")

    try:
        ok = input("\n  Confirma? [s/n]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        ok = "s"

    if ok not in ("s", "sim", "y", "yes", ""):
        return run_config_wizard(df, params)

    return config


# ─── Geração multi-segmento ───────────────────────────────────────────────────

def gerar_relatorios_segmentados(
    df: pd.DataFrame,
    config: dict,
    params: dict,
    out_dir: str,
) -> Dict[str, dict]:
    """
    Gera relatórios para cada segmento.
    Retorna dict{segment_name: relatorio_dict}.
    """
    from .analytics import (
        calcular_kpis, calcular_roi, calcular_funil,
        calcular_status, gerar_insights,
        calcular_funil_dinamico, calcular_tempo_dinamico,
    )
    from datetime import datetime

    segments = config.get("segments") or []
    dim_col  = config.get("dim_col")
    results  = {}

    for seg in segments:
        df_seg = filter_by_segment(df, dim_col, seg)
        if len(df_seg) < 5:
            print(f"   ⚠️  Segmento '{seg}' tem apenas {len(df_seg)} registros — pulado.")
            continue

        kpis     = calcular_kpis(df_seg)
        roi      = calcular_roi(df_seg, params)
        funil    = calcular_funil(df_seg)
        status   = calcular_status(df_seg)
        insights = gerar_insights(kpis, roi)
        funil_din  = calcular_funil_dinamico(df_seg)
        tempos_din = calcular_tempo_dinamico(df_seg)

        results[str(seg)] = {
            "meta": {
                "cliente":   params.get("cliente_nome", "Cliente"),
                "periodo":   params.get("periodo", ""),
                "segmento":  str(seg),
                "dimensao":  config.get("dimension", ""),
                "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "logo_url":  params.get("logo_url", ""),
            },
            "kpis":      kpis,
            "roi":       roi,
            "funil":     funil,
            "funil_din": funil_din,
            "tempos_din": tempos_din,
            "status":    status,
            "insights":  insights,
            "_df":       df_seg,
        }
        print(f"   ✅ Segmento '{seg}': {len(df_seg):,} candidatos | "
              f"{kpis['Com DigAI']['contratados']} contratados")

    return results


def build_summary_table(results: Dict[str, dict]) -> List[Dict]:
    """
    Constrói tabela comparativa de todos os segmentos para o Sumário.
    Inclui volume_pct (% de candidatos que passaram pelo DigAI) e
    perf_vs_media (assertividade relativa à média dos segmentos).
    """
    rows = []
    for seg_name, rel in results.items():
        kpis = rel["kpis"]
        roi  = rel["roi"]
        com  = kpis.get("Com DigAI", {})
        sem  = kpis.get("Sem DigAI", {})
        total_com = com.get("total", 0)
        total_all = total_com + sem.get("total", 0)
        rows.append({
            "segmento":     seg_name,
            "total_com":    total_com,
            "contratados":  com.get("contratados", 0),
            "adesao":       com.get("adesao", 0),
            "assertividade":com.get("assertividade", 0),
            "sla_media":    com.get("sla_media"),
            "saving":       roi.get("savings", 0),
            "roi":          roi.get("roi", 0),
            "veredicto":    rel["insights"]["veredicto"],
            "volume_pct":   total_com / total_all if total_all > 0 else 0,
        })

    # Performance vs média (assertividade deste segmento - média geral)
    valids = [r["assertividade"] for r in rows
              if r["assertividade"] is not None and r["assertividade"] != 0]
    avg_assert = sum(valids) / len(valids) if valids else 0
    for row in rows:
        a = row.get("assertividade")
        row["perf_vs_media"] = (a - avg_assert) if a is not None else None

    # Ordenar por assertividade decrescente (melhor segmento primeiro)
    rows.sort(key=lambda r: r["assertividade"] or 0, reverse=True)
    return rows
