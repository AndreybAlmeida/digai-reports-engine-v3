"""
DigAI Reports Engine — Data Ingestion Layer (ATS-agnóstico)

Carrega e normaliza as fontes de dados:
  1. Arquivo do processo seletivo (qualquer ATS — Gupy, Kenoby, Breezy, etc.)
  2. Base DigAI de entrevistas

A segmentação Com/Sem DigAI SEMPRE vem do chaveamento com a base DigAI
por email e/ou telefone. O ATS apenas fornece o histórico de etapas e status.

Detecção de colunas por padrão semântico, não por nome fixo.
"""

from __future__ import annotations

import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from .schema import IngestionResult, CandidaturaResult, DigAIResult


# ─── Helpers de normalização ──────────────────────────────────────────────────

def normalize_email(s) -> str:
    """
    Normaliza email para lowercase sem espaços (internos ou externos).
    Seguro para qualquer tipo de entrada (NaN, int, float, str).
    Remove espaços internos que aparecem em exports com encoding bugado.
    """
    if s is None:
        return ""
    s_str = str(s).strip().lower()
    if s_str in ("", "nan", "none", "inf", "-inf"):
        return ""
    # Remove espaços internos (ex: "user @domain.com" → "user@domain.com")
    s_str = re.sub(r"\s+", "", s_str)
    # Descarta se não tem @ ou domínio após normalização
    if "@" not in s_str or s_str.startswith("@") or s_str.endswith("@"):
        return ""
    return s_str


def normalize_phone(s) -> str:
    """Remove tudo exceto dígitos, normaliza para 10–11 dígitos brasileiros."""
    if pd.isna(s) or str(s).strip() == "":
        return ""
    digits = re.sub(r"\D", "", str(s))
    if len(digits) in (12, 13) and digits.startswith("55"):
        digits = digits[2:]
    return digits[-11:] if len(digits) > 11 else digits


def normalize_cpf(s) -> str:
    if pd.isna(s) or str(s).strip() == "":
        return ""
    cleaned = re.sub(r"\D", "", str(s)).zfill(11)
    return cleaned if len(cleaned) == 11 else ""


def _detect_encoding(path: str) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detect_sep(path: str, encoding: str) -> str:
    with open(path, "r", encoding=encoding, errors="replace") as f:
        first = f.readline()
    return ";" if first.count(";") > first.count(",") else ","


def _read_csv_auto(path: str, **kwargs) -> pd.DataFrame:
    enc = _detect_encoding(path)
    sep = _detect_sep(path, enc)
    df = pd.read_csv(
        path, sep=sep, encoding=enc,
        low_memory=False, on_bad_lines="warn", **kwargs,
    )
    df.columns = df.columns.str.strip()
    return df


def _read_file(path: str, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path, **kwargs)
        df.columns = df.columns.str.strip()
        return df
    return _read_csv_auto(path, **kwargs)


def _get_col(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Retorna df[col] como Series garantida — nunca DataFrame.
    Quando a coluna não existe, retorna Series de strings vazias.
    Protege contra arquivos com colunas duplicadas onde df.get() retorna DataFrame.
    """
    if col not in df.columns:
        return pd.Series("", index=df.index, dtype=str)
    result = df[col]
    if isinstance(result, pd.DataFrame):
        # Coluna duplicada: pega apenas a primeira ocorrência
        result = result.iloc[:, 0]
    return result


def _to_date(series: pd.Series) -> pd.Series:
    """Converte para datetime tolerando formatos mistos (pandas 2.x safe)."""
    try:
        return pd.to_datetime(series, errors="coerce", dayfirst=True)
    except (ValueError, TypeError):
        try:
            return pd.to_datetime(series, errors="coerce", format="mixed", dayfirst=True)
        except Exception:
            return pd.Series(pd.NaT, index=series.index)


# ─── Detecção semântica de colunas (ATS-agnóstico) ───────────────────────────

# Padrões semânticos: chave → lista de regex que matcham o nome da coluna
SEMANTIC_PATTERNS = {
    "email": [
        r"e[-_]?mail", r"^email$", r"e-mail\s*do\s*candidato",
        r"endere[cç]o\s*de\s*e-?mail", r"email\s*address", r"user\s*email",
        r"contact\s*email", r"candidate\s*email",
    ],
    "phone": [
        r"celular", r"telefone", r"phone", r"fone", r"cel\b",
        r"mobile", r"whatsapp", r"contato\s*telef", r"n[uú]mero\s*de\s*telefone",
        r"tel\b", r"phone\s*number",
    ],
    "cpf": [
        r"^cpf$", r"cpf\s*do\s*candidato", r"documento\s*cpf",
        r"n[uú]mero\s*do\s*cpf", r"tax\s*id",
    ],
    "nome": [
        r"nome\s*do\s*candidato", r"candidato\s*nome", r"^nome$",
        r"^candidato$", r"\bname\b", r"nome\s*completo", r"full\s*name",
        r"candidate\s*name",
    ],
    "vaga": [r"nome\s*da\s*vaga", r"vaga", r"job\s*name", r"position", r"cargo\s*vaga"],
    "cargo": [r"^cargo$", r"^func[aã]o$", r"^posi[cç][aã]o$", r"role"],
    "area": [r"[aá]rea\s*da\s*vaga", r"[aá]rea\s*do\s*processo", r"department", r"^[aá]rea$"],
    "filial": [r"filial", r"unidade", r"localidade", r"branch", r"site"],
    "recrutador": [r"respons[aá]vel", r"recrut", r"owner", r"gestor"],
    "status": [
        r"status\s*na\s*vaga", r"status\s*do\s*candidato", r"^status$",
        r"situa[cç][aã]o", r"status\s*atual",  # ← FIX CRÍTICO: multi-ATS
        r"current\s*status", r"stage\s*status",
        r"candidature\s*status", r"outcome", r"resultado",
    ],
    "etapa_atual": [r"etapa\s*atual", r"current\s*stage", r"fase\s*atual"],
    "data_cadastro": [
        r"data\s*de\s*inscri[cç][aã]o", r"data\s*inscri[cç][aã]o", r"applied[\s_]?at",
        r"data\s*cadastro", r"created[\s_]?at", r"data\s*entrada",
    ],
    "data_final": [r"data\s*final", r"data\s*(de\s*)?(reprova|contrat|desist)", r"final[\s_]?date"],
    "data_contratacao": [
        r"data\s*de\s*movimenta[cç][aã]o\s*para\s*contrata",
        r"data\s*(informada\s*de\s*)?contrata[cç][aã]o",
        r"hire[\s_]?date", r"data\s*admiss[aã]o",
        r"hired[\s_]?at", r"data\s*de\s*admiss[aã]o",
        r"admission\s*date", r"start\s*date",
    ],
}

# Wildcard para detectar etapa Entrevista Inteligente (DigAI no Gupy)
EI_PATTERN = re.compile(r"entrev.*intelig", re.IGNORECASE)

MAX_STAGES = 25


def _find_col(df: pd.DataFrame, semantic_key: str) -> Optional[str]:
    """Retorna o nome da coluna no df que melhor corresponde ao semantic_key."""
    patterns = SEMANTIC_PATTERNS.get(semantic_key, [])
    for col in df.columns:
        col_lower = col.strip().lower()
        for pat in patterns:
            if re.search(pat, col_lower, re.IGNORECASE):
                return col
    return None


def _detect_by_content(df: pd.DataFrame, semantic_key: str) -> Optional[str]:
    """
    Detecta coluna pelo conteúdo quando header semântico não bate.
    Tentativa 2 da arquitetura de detecção (após SEMANTIC_PATTERNS).
    """
    if semantic_key == "email":
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(50)
            if sample.str.contains(r"@.+\.", regex=True).mean() >= 0.5:
                return col
    elif semantic_key == "phone":
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(50)
            if sample.str.replace(r"\D", "", regex=True).str.len().between(10, 13).mean() >= 0.6:
                return col
    elif semantic_key == "cpf":
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(50)
            if sample.str.replace(r"\D", "", regex=True).str.len().eq(11).mean() >= 0.7:
                return col
    return None


def _detect_stage_cols(df: pd.DataFrame) -> dict:
    """
    Detecta colunas de etapas dinâmicas.
    Suporta padrões Gupy (Etapa N / Data de entrada na etapa N)
    e padrões genéricos (Stage N / Stage N Entry Date).
    """
    stage_cols = {}
    for col in df.columns:
        cl = col.strip()
        # Gupy PT
        for pat, key in [
            (r"^Etapa\s+(\d+)$",                          "name"),
            (r"^Data\s+de\s+entrada\s+na\s+etapa\s+(\d+)", "entry"),
            (r"^Data\s+de\s+saída\s+da\s+etapa\s+(\d+)",   "exit"),
            (r"^Tempo\s+na\s+etapa\s+(\d+)",               "days"),
            # Genérico EN
            (r"^Stage\s+(\d+)\s*Name",                    "name"),
            (r"^Stage\s+(\d+)\s*Entry",                   "entry"),
            (r"^Stage\s+(\d+)\s*Exit",                    "exit"),
            (r"^Stage\s+(\d+)\s*Days",                    "days"),
            (r"^Stage\s+(\d+)$",                          "name"),
        ]:
            m = re.match(pat, cl, re.IGNORECASE)
            if m:
                n = int(m.group(1))
                stage_cols.setdefault(n, {})[key] = col
                break
    return stage_cols


# ─── Loader principal: qualquer arquivo de processo seletivo ──────────────────

def load_pipeline(path: str) -> pd.DataFrame:
    """
    Carrega o arquivo do processo seletivo de QUALQUER ATS.

    Detecta semanticamente:
    - email, telefone, CPF (chaves de join com DigAI)
    - status do candidato
    - datas relevantes
    - etapas do funil (dinâmicas, qualquer nomenclatura)
    - dimensões: área, filial, recrutador, cargo, vaga

    Retorna DataFrame normalizado com colunas padronizadas.
    df.attrs['stage_cols']    → dict{n: {name, entry, exit, days}}
    df.attrs['ei_stage_col']  → coluna de entrada da EI no ATS (ou None)
    df.attrs['dims_detected'] → dict de dimensões encontradas
    """
    df = _read_file(path)

    # Remove colunas completamente vazias (bug de exportação Gupy e outros ATS)
    bug_cols = [c for c in df.columns if re.match(r"^Coluna\s*\d+$", str(c).strip())]
    if bug_cols:
        df = df.drop(columns=bug_cols)
        print(f"   🧹 {len(bug_cols)} colunas vazias removidas")

    # Remove colunas totalmente nulas
    df = df.dropna(axis=1, how="all")

    # ── Mapeamento semântico de colunas ───────────────────────────────────────
    rename = {}
    for semantic_key in SEMANTIC_PATTERNS:
        col = _find_col(df, semantic_key)
        if col and col not in rename:
            rename[col] = semantic_key

    # ── Fallback: detecção por conteúdo (Tentativa 2) ────────────────────────
    # Se header semântico não encontrou, tenta detectar pelo conteúdo
    for key, target in [("email", "email"), ("phone", "phone"), ("cpf", "cpf")]:
        if target not in rename:
            col = _detect_by_content(df, key)
            if col and col not in rename:
                rename[col] = target
                print(f"   🔍 Deteccao por conteudo: coluna '{col}' → {key}")

    df = df.rename(columns=rename)

    # ── Etapas dinâmicas ──────────────────────────────────────────────────────
    stage_cols = _detect_stage_cols(df)

    for n, cols in stage_cols.items():
        for key in ("entry", "exit"):
            if key in cols and cols[key] in df.columns:
                df[cols[key]] = _to_date(df[cols[key]])

    # ── Datas canônicas ───────────────────────────────────────────────────────
    for col in ("data_cadastro", "data_final", "data_contratacao"):
        if col in df.columns:
            df[col] = _to_date(df[col])

    # data_cadastro fallback: primeira etapa de entrada
    if "data_cadastro" not in df.columns:
        first_entry = stage_cols.get(1, {}).get("entry")
        if first_entry and first_entry in df.columns:
            df["data_cadastro"] = df[first_entry]
        else:
            df["data_cadastro"] = pd.NaT

    # ── Chaves de join ────────────────────────────────────────────────────────
    df["email"] = _get_col(df, "email").apply(normalize_email)
    df["phone"] = _get_col(df, "phone").apply(normalize_phone)
    df["cpf"]   = _get_col(df, "cpf").apply(normalize_cpf)

    # ── Detectar EI no ATS (opcional) ────────────────────────────────────────
    ei_stage_col = None
    for n in sorted(stage_cols.keys()):
        name_col = stage_cols[n].get("name")
        if name_col and name_col in df.columns:
            for val in df[name_col].dropna().unique():
                if EI_PATTERN.search(str(val)):
                    ei_stage_col = stage_cols[n].get("entry")
                    break
        if ei_stage_col:
            break

    # ── Dimensões disponíveis ─────────────────────────────────────────────────
    dims_detected = {}
    for dim in ("area", "filial", "recrutador", "cargo", "vaga"):
        if dim in df.columns:
            vals = df[dim].dropna().astype(str).str.strip()
            vals = vals[vals != ""].unique().tolist()
            if 2 <= len(vals) <= 100:
                dims_detected[dim] = sorted(vals)

    # Período
    if "data_cadastro" in df.columns:
        periodos = (
            pd.to_datetime(df["data_cadastro"], errors="coerce")
            .dropna().dt.to_period("M").astype(str).unique().tolist()
        )
        periodos = sorted([p for p in periodos if p != "NaT"])
        if len(periodos) >= 2:
            df["_periodo"] = (
                pd.to_datetime(df["data_cadastro"], errors="coerce")
                .dt.to_period("M").astype(str)
            )
            dims_detected["periodo"] = periodos

    # Mantém attrs para compatibilidade com código legado que lê df.attrs diretamente
    df.attrs["stage_cols"]    = stage_cols
    df.attrs["ei_stage_col"]  = ei_stage_col
    df.attrs["dims_detected"] = dims_detected

    n_stages = len(stage_cols)
    ei_info  = f"EI no ATS: {ei_stage_col}" if ei_stage_col else "EI: apenas via base DigAI"
    dims_str = ", ".join(dims_detected.keys()) or "nenhuma"
    print(f"   📊 Pipeline: {len(df):,} candidatos | {n_stages} etapas | {ei_info}")
    print(f"   📐 Dimensões detectadas: {dims_str}")

    # Retorna IngestionResult com metadados explícitos (não dependem de df.attrs)
    return IngestionResult(
        df=df,
        stage_cols=stage_cols,
        ei_stage_col=ei_stage_col,
        dims_detected=dims_detected,
    )


# ─── Loader legado (backward compat) ─────────────────────────────────────────

def load_gupy_funnel(path: str) -> IngestionResult:
    """Alias legado para load_pipeline."""
    return load_pipeline(path)


def load_gupy_candidatura(path: str) -> CandidaturaResult:
    """
    Carrega relatório complementar (Pasta1 Gupy, Relatório de Contratações ou equivalente).

    Aceita tanto o "Relatório de Candidatura" quanto o "Relatório de Contratações" do Gupy
    — ambos têm estrutura plana com email, data de contratação e opcionalmente coluna Tags.

    Extrai phone/email/candidato_id e status de contratação para enriquecer o pipeline.
    """
    df = _read_file(path)
    df = df.dropna(axis=1, how="all")

    rename = {}
    for semantic_key in ("email", "phone", "cpf", "nome", "data_cadastro",
                         "data_contratacao", "vaga", "cargo", "area", "filial"):
        col = _find_col(df, semantic_key)
        if col and col not in rename:
            rename[col] = f"{semantic_key}_cand"

    # candidato_id
    for col in df.columns:
        if re.search(r"id\s*(da\s*)?inscri[cç][aã]o|inscri[cç][aã]o\s*id|candidate[\s_]?id", col, re.I):
            rename[col] = "candidato_id"
            break

    # Tags column (Gupy embeds status like "[Contratado, Onboarding]")
    tags_col = None
    for col in df.columns:
        if re.search(r"^tags?$", col.strip(), re.IGNORECASE):
            tags_col = col
            break

    df = df.rename(columns=rename)

    for col in ("data_cadastro_cand", "data_contratacao_cand"):
        if col in df.columns:
            df[col] = _to_date(df[col])

    df["email"] = _get_col(df, "email_cand").apply(normalize_email)
    df["phone"] = _get_col(df, "phone_cand").apply(normalize_phone)

    # Extrai status a partir da coluna Tags quando disponível
    # Ex: "[Contratado, Onboarding]" → "Contratado"
    if tags_col and tags_col in df.columns:
        HIRED_TAGS = re.compile(r"contrat(ado|ando|a[cç][aã]o)|hired|admitido", re.IGNORECASE)
        df["status_cand"] = df[tags_col].astype(str).apply(
            lambda t: "Contratado" if HIRED_TAGS.search(t) else ""
        )
        n_hired_tags = (df["status_cand"] == "Contratado").sum()
        if n_hired_tags > 0:
            print(f"   🏷️  Tags: {n_hired_tags:,} contratados detectados via coluna Tags")

    # Fallback: se tem data_contratacao mas não tem status_cand, infere Contratado
    if "data_contratacao_cand" in df.columns:
        if "status_cand" not in df.columns:
            df["status_cand"] = ""
        mask = (
            df["data_contratacao_cand"].notna() &
            (df["status_cand"] == "")
        )
        df.loc[mask, "status_cand"] = "Contratado"

    n_contratados = (_get_col(df, "status_cand") == "Contratado").sum()
    print(f"   📋 Complementar: {len(df):,} registros | {n_contratados:,} contratados")

    # Auto-detecção de arquivo de contratações: se >=80% dos registros têm
    # status_cand="Contratado" é quase certo que é um Relatório de Contratações.
    # Ativa is_contratados=True para usar como âncora definitiva na segmentação.
    is_anchor = len(df) > 0 and (n_contratados / len(df)) >= 0.80
    if is_anchor:
        print(f"   🔒 Arquivo identificado como Relatório de Contratações "
              f"({n_contratados / len(df):.0%} contratados) — modo âncora ativado")
    return CandidaturaResult(df=df, is_contratados=is_anchor)


def load_contratacoes(path: str) -> CandidaturaResult:
    """
    Carrega o Relatório de Contratações do Gupy (ou equivalente de qualquer ATS).

    Diferente de load_gupy_candidatura(): este loader assume que TODOS os registros
    são contratados confirmados e retorna is_contratados=True, ativando o modo de
    âncora definitiva em build_unified().

    Quando is_contratados=True, candidatos do funil que NÃO estejam neste arquivo
    não podem ser promovidos a "Contratado" por inferência — eliminando os 4 fallbacks
    encadeados de status do segmentation.py.
    """
    result = load_gupy_candidatura(path)

    # Força todos os registros como Contratado — é isso que um relatório de
    # contratações contém por definição.
    if "status_cand" not in result.df.columns:
        result.df["status_cand"] = "Contratado"
    else:
        result.df["status_cand"] = "Contratado"

    # Auto-detecção: mesmo que o chamador use load_gupy_candidatura() diretamente,
    # se >80% dos registros com status são Contratado, eleva para modo âncora.
    # Isso garante que clientes que não atualizam o código ainda se beneficiam.
    n_total  = len(result.df)
    n_cont   = (_get_col(result.df, "status_cand") == "Contratado").sum()
    is_anchor = n_total > 0 and (n_cont / n_total) >= 0.80

    return CandidaturaResult(df=result.df, is_contratados=is_anchor or True)


# ─── Loader: DigAI Base ───────────────────────────────────────────────────────

def load_digai_base(path: str) -> "DigAIResult":
    """
    Carrega a base de entrevistas DigAI.
    Chaves de join: email, CPF, phoneNumber.

    Detecta automaticamente se o arquivo é um export da plataforma DigAI ou
    um export de Candidaturas do Gupy. No segundo caso, is_gupy_candidature=True
    e o chamador (segmentation.py) ignora o match por email e usa o ei_stage_col
    do ATS como fallback de segmentação.
    """
    from .schema import DigAIResult

    df = _read_file(path)
    df = df.dropna(axis=1, how="all")

    cols_lower = {c.strip().lower() for c in df.columns}
    print(f"   🤖 DigAI Base raw — colunas: {list(df.columns)}")

    # ── Detecta se é export do Gupy Candidaturas (não é a base DigAI) ───────────
    # Indicadores fortes de export Gupy: colunas exclusivas do ATS que a plataforma
    # DigAI nunca exportaria.
    _GUPY_INDICATORS = {
        "etapa atual", "status da etapa", "status na vaga",
        "motivo de reprovação", "candidatura rápida",
        "como encontrou a vaga", "tipo de deficiência",
    }
    _gupy_hits = sum(1 for c in _GUPY_INDICATORS if c in cols_lower)

    # Indicadores de export legítimo da plataforma DigAI
    _DIGAI_INDICATORS = {
        "hasapproved", "hasaproved", "triagemdostatus", "triagemstatus",
        "appliedat", "interviewdate", "scoreia", "score_ia",
        "aiinitialscore", "ranking_ia", "rankingai",
    }
    _digai_hits = sum(
        1 for c in df.columns
        if c.strip().lower().replace(" ", "").replace("_", "").replace("-", "") in _DIGAI_INDICATORS
    )

    is_gupy_candidature = (_gupy_hits >= 2) and (_digai_hits == 0)
    if is_gupy_candidature:
        print(
            f"   ⚠️  ATENÇÃO: o arquivo de 'base DigAI' parece ser um export do Gupy Candidaturas "
            f"(detectados {_gupy_hits} indicadores Gupy, {_digai_hits} indicadores DigAI).\n"
            f"   ⚠️  Para segmentação correta, forneça o export da PLATAFORMA DigAI "
            f"(colunas: email, score, hasApproved, appliedAt).\n"
            f"   ⚠️  O sistema usará a etapa 'Entrevista Inteligente' do ATS como fallback."
        )

    rename = {}
    for c in df.columns:
        cll = c.strip().lower().replace(" ", "").replace("_", "").replace("-", "")
        # Email — múltiplos formatos
        if cll in ("email", "emailaddress", "email_address", "useremail", "e-mail", "emaildousuario"):
            rename[c] = "email_raw"
        # CPF — chave secundária de join
        elif cll in ("cpf", "cpfdocandidato", "cpfdousuario", "numerodocpf", "documentocpf"):
            rename[c] = "cpf_raw"
        # Phone — múltiplos formatos
        elif cll in ("phonenumber", "phone", "celular", "telefone", "fone", "cel", "numerodetelefone"):
            rename[c] = "phone_raw"
        # Nome completo do candidato — usado para inferência de gênero
        elif cll in ("nomecompleto", "nomecompletodocandidato", "fullname", "nome"):
            rename[c] = "nome_digai"
        # Estado/UF — usado para origem geográfica sem precisar de DDD
        elif cll in ("estado", "uf", "state", "provincia"):
            rename[c] = "estado_digai"
        # Cidade
        elif cll in ("cidade", "city", "municipio"):
            rename[c] = "cidade_digai"
        # Nome da vaga/triagem
        elif cll in ("triagemname", "triagemename", "vaganame", "jobname", "jobposition",
                     "vagadigai", "nomevaga", "vaga"):
            rename[c] = "vaga_digai"
        # Status da triagem
        elif cll in ("triagemstatus", "triagemestatus", "statusia", "triagemdostatus"):
            rename[c] = "status_ia"
        # Aprovado?
        elif cll in ("hasapproved", "hasaproved", "approved", "aprovado", "aprovadoia"):
            rename[c] = "aprovado_ia_raw"
        # Data da entrevista DigAI — padrões específicos da plataforma DigAI.
        # NÃO usa fallback genérico "data" para evitar capturar "Data de inscrição" do Gupy.
        elif cll in ("appliedat", "applieddate", "dataentrevista", "dataei", "datacriacao",
                     "interviewdate", "created_at", "createdat", "dataaplicacao"):
            rename[c] = "data_ei_raw"
        # Score DigAI — inclui "Score (Afinidade)" do Gupy integrado e variantes
        elif cll in ("score", "scoreia", "pontuacao", "nota", "score(afinidade)",
                     "scoreafinidade", "afinidade", "matching"):
            rename[c] = "score_ia"
        # Score inicial
        elif cll in ("aiinitialscore", "initialscore", "scoreinicial", "notainicial"):
            rename[c] = "score_inicial_ia"
        elif cll in ("companyname", "empresa", "company"):
            rename[c] = "empresa"
        elif cll in ("userid", "id", "candidateid"):
            rename[c] = "digai_user_id"
        elif cll in ("ranking", "rankingai", "rankia"):
            rename[c] = "ranking_ia"
        elif cll in ("haseditedscored", "haseditedscored", "scoreeditado", "scoreditado",
                     "editedscore", "editedscored", "scoreedited"):
            rename[c] = "score_editado"
        elif cll in ("requirement1ismet", "requirementismet", "requirement_1_is_met",
                     "requisitoatendido", "reqatendido", "isrequirementmet"):
            rename[c] = "req_atendido"
        elif cll in ("daystayopen", "daysopen", "daysopened", "diasaberta",
                     "diasvagaaberta", "diasvaga", "daysstayopen"):
            rename[c] = "dias_vaga"
        elif cll in ("workspace", "worksapce"):
            rename[c] = "workspace"

    # firstname + lastname → nome_digai (DigAI export padrão)
    if "nome_digai" not in rename.values():
        first_col = next((c for c in df.columns
                         if c.strip().lower().replace("_","") in ("firstname","firstname","givenname")), None)
        last_col  = next((c for c in df.columns
                         if c.strip().lower().replace("_","") in ("lastname","familyname","surname","sobrenome")), None)
        if first_col or last_col:
            fc = df[first_col].fillna("").astype(str) if first_col else pd.Series("", index=df.index)
            lc = df[last_col].fillna("").astype(str)  if last_col  else pd.Series("", index=df.index)
            df["nome_digai"] = (fc + " " + lc).str.strip()

    # Detecção semântica de email se não encontrou ainda
    if "email_raw" not in rename.values():
        for c in df.columns:
            if re.search(r"e[-_]?mail", c, re.IGNORECASE):
                rename[c] = "email_raw"
                break

    # Detecção semântica de data_ei — apenas padrões DigAI-específicos (não "data" genérico)
    if "data_ei_raw" not in rename.values():
        for c in df.columns:
            if re.search(r"(applied|entrevista\s+ia|entrevistadigai|interviewdate|dataei\b)", c, re.IGNORECASE):
                rename[c] = "data_ei_raw"
                break

    # ── Fallback: detecção por conteúdo (Tentativa 2) ────────────────────────
    for key, target in [("email", "email_raw"), ("phone", "phone_raw"), ("cpf", "cpf_raw")]:
        if target not in rename.values():
            col = _detect_by_content(df, key)
            if col and col not in rename:
                rename[col] = target
                print(f"   🔍 Deteccao por conteudo (DigAI): coluna '{col}' → {key}")

    df = df.rename(columns=rename)

    df["data_ei"] = _to_date(df["data_ei_raw"]) if "data_ei_raw" in df.columns else pd.NaT

    if "aprovado_ia_raw" in df.columns:
        df["aprovado_ia"] = df["aprovado_ia_raw"].astype(str).str.lower().isin(
            ["true", "1", "sim", "yes", "aprovado"]
        )

    df["email"] = _get_col(df, "email_raw").apply(normalize_email)
    df["phone"] = _get_col(df, "phone_raw").apply(normalize_phone)
    df["cpf"]   = _get_col(df, "cpf_raw").apply(normalize_cpf)

    # Marca todas as linhas como pertencentes à base DigAI (flag de match)
    df["_in_digai"] = True

    n_with_email = df["email"].ne("").sum()
    n_with_cpf   = df["cpf"].ne("").sum()
    print(f"   🤖 DigAI Base: {len(df):,} registros | {n_with_email:,} com email | {n_with_cpf:,} com CPF")
    if n_with_email > 0:
        sample = df[df["email"].ne("")]["email"].head(3).tolist()
        print(f"   📧 Amostra emails DigAI: {sample}")

    # Salva total antes da deduplicação — usado no cálculo de ROI
    total_before_dedup = len(df)

    # Deduplica por email — mantém o registro com maior score (spec LOGICA_CRUZAMENTO C5).
    if df["email"].ne("").any():
        sort_cols, sort_asc = [], []
        if "score_ia" in df.columns:
            sort_cols.append("score_ia");  sort_asc.append(False)
        sort_cols.append("data_ei");       sort_asc.append(False)
        df = (df.sort_values(sort_cols, ascending=sort_asc, na_position="last")
                .drop_duplicates(subset=["email"], keep="first")
                .reset_index(drop=True))

    return DigAIResult(df=df, total=total_before_dedup, is_gupy_candidature=is_gupy_candidature)
