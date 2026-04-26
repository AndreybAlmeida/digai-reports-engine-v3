"""
DigAI Reports Engine — Segmentation & Data Unification

Junta as 3 fontes (Gupy Funnel, Gupy Candidatura, DigAI Base)
e classifica cada candidato como "Com DigAI" ou "Sem DigAI".

Estratégias de segmentação (auto-detectadas):
  A. Gupy-stage: existe etapa "Entrevista Inteligente" no Gupy
     → Com DigAI = quem tem data de entrada nessa etapa
  B. DigAI cross-ref: não existe EI no Gupy
     → Com DigAI = email ou telefone encontrado na base DigAI

Saída: DataFrame unificado compatível com analytics.py:
  candidato_id, email, phone, cpf, nome, vaga,
  processo_seletivo, status,
  data_cadastro, data_ei, data_final, data_contratacao,
  score_ia, aprovado_ia, ranking_ia,
  stage_N_name, stage_N_entry, stage_N_exit, stage_N_days  (N=1..max)
"""

import gc
import re
import pandas as pd
import numpy as np
from .ingestion import normalize_email, normalize_phone
from .schema import IngestionResult, CandidaturaResult, DigAIResult, SegmentationResult

COM_DIGAI  = "Com DigAI"
SEM_DIGAI  = "Sem DigAI"

# Padrões de nomes de etapa que indicam contratação
_HIRED_STAGE_RE = re.compile(
    r"contrat(ado|ando|a[cç][aã]o)|hired|admitido|admiss[aã]o|proposta\s*aceita",
    re.IGNORECASE,
)

# Mapa de status Gupy → status canônico
# IMPORTANTE: "aprovado" NÃO mapeia para "Contratado".
# No Gupy, "Aprovado" significa "avançou para a próxima etapa" — não contratação.
# Usar "aprovado" → "Contratado" inflava o count de contratados em 40-60%.
STATUS_MAP = {
    "contratado":       "Contratado",
    "hired":            "Contratado",
    "admitido":         "Contratado",
    "reprovado":        "Reprovado",
    "não aprovado":     "Reprovado",
    "nao aprovado":     "Reprovado",
    "rejected":         "Reprovado",
    "arquivado":        "Reprovado",
    "eliminado":        "Reprovado",
    "desistência":      "Desistiu",
    "desistencia":      "Desistiu",
    "desistiu":         "Desistiu",
    "withdrew":         "Desistiu",
    "em processo":      "Em processo",
    "aprovado":         "Em processo",   # Gupy: avançou de etapa, NÃO contratado
    "ativo":            "Em processo",
    "active":           "Em processo",
    "em andamento":     "Em processo",
    "em análise":       "Em processo",
    "em analise":       "Em processo",
}


def _map_status(raw_status: pd.Series) -> pd.Series:
    normalized = raw_status.fillna("").astype(str).str.strip().str.lower()
    return normalized.map(STATUS_MAP).fillna("Em processo").astype("category")


def _infer_hired_from_funnel(df: pd.DataFrame, stage_cols: dict) -> pd.Series:
    """
    Fallback: quando não há arquivo complementar, infere candidatos contratados
    a partir das etapas do funil e/ou da coluna de status.

    Retorna uma Series booleana (True = candidato contratado).
    """
    hired_mask = pd.Series(False, index=df.index)

    # 1. Status canônico já mapeado
    if "status" in df.columns:
        hired_mask |= df["status"].astype(str).str.lower().str.strip() == "contratado"

    # 2. etapa_atual contém "contratad*" / "contratando" / etc.
    if "etapa_atual" in df.columns:
        hired_mask |= df["etapa_atual"].astype(str).apply(
            lambda v: bool(_HIRED_STAGE_RE.search(v))
        )

    # 3. Qualquer nome de etapa dinâmica contém padrão de contratação
    for n, cols in stage_cols.items():
        name_col = cols.get("name")
        if name_col and name_col in df.columns:
            hired_mask |= df[name_col].astype(str).apply(
                lambda v: bool(_HIRED_STAGE_RE.search(v))
            )

    return hired_mask


def _infer_data_contratacao_from_stages(df: pd.DataFrame, stage_cols: dict) -> pd.Series:
    """
    Infere data_contratacao a partir da data de entrada na etapa de contratação
    quando nenhum arquivo complementar está disponível.
    """
    result = pd.Series(pd.NaT, index=df.index)

    for n in sorted(stage_cols.keys(), reverse=True):
        cols = stage_cols[n]
        name_col  = cols.get("name")
        entry_col = cols.get("entry")
        if name_col and entry_col and name_col in df.columns and entry_col in df.columns:
            mask = df[name_col].astype(str).apply(lambda v: bool(_HIRED_STAGE_RE.search(v)))
            result = result.where(~mask, df[entry_col])

    return result


def build_unified(
    funnel_df,
    candidatura_df=None,
    digai_df=None,
) -> SegmentationResult:
    """
    Constrói o DataFrame unificado a partir das 3 fontes.

    Parameters
    ----------
    funnel_df       : IngestionResult ou pd.DataFrame (load_gupy_funnel())
    candidatura_df  : CandidaturaResult ou pd.DataFrame (load_gupy_candidatura()) — opcional
    digai_df        : DigAIResult ou pd.DataFrame (load_digai_base()) — opcional

    Returns
    -------
    SegmentationResult com DataFrame unificado e metadados explícitos.
    """
    # ── Desempacota tipos formais (retrocompatível com DataFrames diretos) ─────
    _dims_detected = {}
    if isinstance(funnel_df, IngestionResult):
        stage_cols    = funnel_df.stage_cols
        ei_stage_col  = funnel_df.ei_stage_col
        _dims_detected = funnel_df.dims_detected
        funnel_df     = funnel_df.df
    else:
        stage_cols   = funnel_df.attrs.get("stage_cols", {})
        ei_stage_col = funnel_df.attrs.get("ei_stage_col")
        _dims_detected = funnel_df.attrs.get("dims_detected", {})

    _total_digai_base = 0
    _is_gupy_candidature = False
    if isinstance(digai_df, DigAIResult):
        _total_digai_base = digai_df.total
        _is_gupy_candidature = digai_df.is_gupy_candidature
        # Se o arquivo detectado é um Gupy Candidaturas (não a plataforma DigAI),
        # descarta o email match — usa ei_stage_col como fallback de segmentação.
        if _is_gupy_candidature:
            digai_df = None
            _total_digai_base = 0
        else:
            digai_df = digai_df.df if digai_df.df is not None else None

    # Extrai flag de âncora antes de desempacotar o DataFrame
    _is_contratados_anchor = False
    if isinstance(candidatura_df, CandidaturaResult):
        _is_contratados_anchor = candidatura_df.is_contratados
        candidatura_df = candidatura_df.df

    # ── Detecta se o arquivo de etapas tem emails individuais ─────────────────
    # Relatórios por VAGA (ex: Gupy Pipeline Report) não têm email de candidato.
    # Nesse caso, o arquivo de candidaturas é a base primária de candidatos.
    funnel_has_emails = (
        "email" in funnel_df.columns and
        funnel_df["email"].ne("").sum() > 0
    )
    cand_has_emails = (
        candidatura_df is not None and
        "email" in candidatura_df.columns and
        candidatura_df["email"].ne("").sum() > 0
    )

    if not funnel_has_emails:
        # Diagnóstico: mostra colunas candidatas a email no funnel
        email_candidates = [c for c in funnel_df.columns if any(
            kw in c.lower() for kw in ("mail", "email", "e-mail")
        )]
        print(f"   🔍 Diagnóstico email funnel — 'email' col existe: {'email' in funnel_df.columns} | "
              f"colunas candidatas: {email_candidates[:10]}")
        if "email" in funnel_df.columns:
            sample = funnel_df["email"].head(5).tolist()
            print(f"   🔍 Amostra coluna 'email' funnel: {sample}")
        else:
            # Mostra primeiras 20 colunas do funnel para diagnóstico
            print(f"   🔍 Primeiras colunas do funnel: {list(funnel_df.columns)[:20]}")

    if not funnel_has_emails and cand_has_emails:
        print("⚠️  Arquivo de etapas sem emails individuais (relatório por vaga).")
        print("    Usando candidaturas como base primária de candidatos.")
        print(f"    ℹ️  Métricas de funil por etapa não disponíveis para este formato.")

        # Usa candidatura como base principal
        df = candidatura_df.copy()

        # Renomeia colunas _cand para canonical (se não conflitar)
        for col in list(df.columns):
            if col.endswith("_cand"):
                base = col[:-5]
                if base not in df.columns:
                    df = df.rename(columns={col: base})

        # Propaga attrs do funnel para manter compatibilidade
        df.attrs["stage_cols"]    = {}   # sem etapas individuais
        df.attrs["ei_stage_col"]  = None
        df.attrs["dims_detected"] = funnel_df.attrs.get("dims_detected", {})

        # Garante phone
        if "phone" not in df.columns:
            df["phone"] = (df["phone_cand"] if "phone_cand" in df.columns else pd.Series("", index=df.index)).fillna("")

        # Status: candidatura file = contratados (tem data de contratação)
        if "status" not in df.columns:
            df["status"] = ""
        # Se tem data_contratacao → marca como Contratado
        dc_col = next((c for c in df.columns if "data_contratacao" in c.lower() or
                       "data de contrata" in c.lower()), None)
        if dc_col and "status" in df.columns:
            mask_hired = df[dc_col].notna() & (df[dc_col].astype(str).str.strip() != "")
            df.loc[mask_hired & (df["status"] == ""), "status"] = "Contratado"

        # Pula o bloco de join candidatura (já é a base)
        _skip_cand_join = True
    else:
        if not funnel_has_emails:
            print("⚠️  Arquivo de etapas sem emails e sem candidaturas complementares.")
            print("    O chaveamento com a base DigAI não será possível.")
        # ── 1. Base = Arquivo de Etapas ───────────────────────────────────────
        # Sem .copy(): funnel_df é deletado no caller após build_unified, então
        # usar a referência direta evita duplicar o maior DataFrame em memória.
        df = funnel_df
        _skip_cand_join = False

    # ── 2. Join Candidatura (email) → phone, candidato_id ─────────────────────
    if not _skip_cand_join and candidatura_df is not None and len(candidatura_df) > 0:
        keep = [c for c in ("email", "phone", "candidato_id", "nome",
                             "status_cand", "data_cadastro_cand", "data_contratacao_cand",
                             "vaga_cand", "cargo_cand", "area_cand", "filial_cand")
                if c in candidatura_df.columns]
        if "email" not in keep:
            keep = []
        cand = candidatura_df[keep] if keep else pd.DataFrame()
        cand = cand[cand["email"].ne("")]

        # Dedup por email antes do LEFT JOIN:
        # O arquivo de candidatura pode ter 1:N por email (candidato em múltiplas vagas).
        # Sem dedup, o LEFT JOIN multiplicaria linhas e inflaria o total de candidatos.
        # Prioridade: Contratado > tem data_contratacao > primeiro registro
        n_cand_raw = len(cand)
        if cand.duplicated("email").any():
            sort_keys, sort_asc = [], []
            if "status_cand" in cand.columns:
                # Coloca "Contratado" primeiro (ordena string: Contratado < Z → desc)
                sort_keys.append("status_cand"); sort_asc.append(True)
            if "data_contratacao_cand" in cand.columns:
                sort_keys.append("data_contratacao_cand"); sort_asc.append(False)
            if sort_keys:
                cand = cand.sort_values(sort_keys, ascending=sort_asc, na_position="last")
            cand = cand.drop_duplicates(subset=["email"], keep="first")
            print(f"   🔁 Candidatura dedup: {n_cand_raw:,} → {len(cand):,} registros únicos por email")

        df = df.merge(cand, on="email", how="left", suffixes=("", "_cand"))
        del cand; gc.collect()

        # Preenche phone se veio da candidatura
        if "phone" not in df.columns:
            df["phone"] = df["phone_cand"].fillna("") if "phone_cand" in df.columns else ""
        else:
            df["phone"] = df["phone"].fillna("").replace("", np.nan)
            if "phone_cand" in df.columns:
                df["phone"] = df["phone"].fillna(df["phone_cand"])
            df["phone"] = df["phone"].fillna("")
    elif not _skip_cand_join:
        if "phone" not in df.columns:
            df["phone"] = ""

    # ── 3. Status canônico ─────────────────────────────────────────────────────
    # Aceita qualquer coluna de status (agnóstico de ATS)
    if "status" in df.columns:
        raw_status = df["status"] if not isinstance(df["status"], pd.DataFrame) else df["status"].iloc[:, 0]
    elif "status_gupy" in df.columns:
        raw_status = df["status_gupy"] if not isinstance(df["status_gupy"], pd.DataFrame) else df["status_gupy"].iloc[:, 0]
    else:
        raw_status = pd.Series("", index=df.index)
    df["status"] = _map_status(raw_status)

    # Override: candidatura é fonte definitiva para Contratados
    # (o funil Gupy frequentemente mantém "Em processo" mesmo após contratação)
    if "status_cand" in df.columns:
        cand_hired = df["status_cand"] == "Contratado"
        if cand_hired.any():
            df.loc[cand_hired, "status"] = "Contratado"
            print(f"   ✅ Status override: {int(cand_hired.sum()):,} contratados atualizados via candidatura")
    if "data_contratacao_cand" in df.columns:
        dt_hired = df["data_contratacao_cand"].notna() & (df["status"].astype(str) != "Contratado")
        if dt_hired.any():
            df.loc[dt_hired, "status"] = "Contratado"
            print(f"   ✅ Status override: {int(dt_hired.sum()):,} adicionais via data_contratacao_cand")

    # ── Âncora definitiva de contratados ──────────────────────────────────────
    # Quando o arquivo complementar é um Relatório de Contratações (is_contratados=True),
    # candidatos do funil que NÃO estejam nesse arquivo não podem ser Contratado.
    # Elimina os fallbacks de inferência que podem promover candidatos incorretamente.
    if _is_contratados_anchor and "email" in df.columns:
        # Conjunto de emails confirmados como contratados (vindos do arquivo âncora)
        hired_emails = set(
            df.loc[df["status_cand"] == "Contratado", "email"]
            .replace("", pd.NA).dropna().unique()
        ) if "status_cand" in df.columns else set()

        if hired_emails:
            # Candidatos que o funil classifica como Contratado mas NÃO estão no arquivo âncora
            # → rebaixa para "Em processo" (o ATS nem sempre atualiza o status)
            mask_rebaixar = (
                (df["status"] == "Contratado") &
                (~df["email"].isin(hired_emails))
            )
            n_rebaixados = int(mask_rebaixar.sum())
            if n_rebaixados > 0:
                df.loc[mask_rebaixar, "status"] = "Em processo"
                print(f"   🔒 Âncora contratados: {n_rebaixados:,} candidatos rebaixados para 'Em processo' "
                      f"(não encontrados no Relatório de Contratações)")
            print(f"   🔒 Âncora ativa: {len(hired_emails):,} emails confirmados como contratados")

    # Fallback: sem arquivo complementar → infere contratados por etapas + status do funil
    # (não executa quando há âncora definitiva)
    if not _skip_cand_join and candidatura_df is None and not _is_contratados_anchor:
        hired_mask = _infer_hired_from_funnel(df, stage_cols)
        n_inferred = hired_mask.sum()
        if n_inferred > 0:
            # Promove para Contratado apenas quem ainda não estava classificado
            df.loc[hired_mask, "status"] = "Contratado"
            print(f"   🔍 Fallback contratados: {n_inferred:,} inferidos via etapas/status do funil")

    # ── 4. data_cadastro canônica ──────────────────────────────────────────────
    if "data_cadastro" not in df.columns:
        df["data_cadastro"] = df["data_cadastro_funnel"] if "data_cadastro_funnel" in df.columns else pd.NaT
    if "data_cadastro_cand" in df.columns:
        df["data_cadastro"] = df["data_cadastro"].fillna(df["data_cadastro_cand"])

    # ── 5. data_final e data_contratacao ──────────────────────────────────────
    if "data_final" not in df.columns:
        df["data_final"] = df["data_final_gupy"] if "data_final_gupy" in df.columns else pd.NaT
    if "data_contratacao" not in df.columns:
        df["data_contratacao"] = df["data_contratacao_gupy"] if "data_contratacao_gupy" in df.columns else pd.NaT
    if "data_contratacao_cand" in df.columns:
        df["data_contratacao"] = df["data_contratacao"].fillna(df["data_contratacao_cand"])

    # Fallback: sem arquivo complementar → infere data_contratacao pela etapa de contratação
    if not _skip_cand_join and candidatura_df is None and stage_cols:
        inferred_dates = _infer_data_contratacao_from_stages(df, stage_cols)
        df["data_contratacao"] = df["data_contratacao"].fillna(inferred_dates)

    # Fallback adicional: aprovado_ia=True na base DigAI → marca como Contratado se ainda sem status
    if digai_df is not None and candidatura_df is None and "aprovado_ia" in df.columns:
        mask_aprovado = df["aprovado_ia"].fillna(False).astype(bool)
        mask_sem_status = df["status"].astype(str).isin(["Em processo", ""])
        upgrade_mask = mask_aprovado & mask_sem_status
        if upgrade_mask.any():
            df.loc[upgrade_mask, "status"] = "Contratado"
            print(f"   🤖 Fallback DigAI: {upgrade_mask.sum():,} aprovados na EI promovidos a Contratado")

    # Para Contratados: data_final = data_contratacao se não tiver
    mask_hired = df["status"] == "Contratado"
    df.loc[mask_hired, "data_final"] = (
        df.loc[mask_hired, "data_final"].fillna(df.loc[mask_hired, "data_contratacao"])
    )

    # ── 6. Colunas de etapas dinâmicas (stage_N_*) ───────────────────────────
    for n in sorted(stage_cols.keys()):
        cols = stage_cols[n]
        src_name  = cols.get("name")
        src_entry = cols.get("entry")
        src_exit  = cols.get("exit")
        src_days  = cols.get("days")

        df[f"stage_{n}_name"]  = df[src_name].astype(str).str.strip()  if src_name  and src_name  in df.columns else ""
        df[f"stage_{n}_entry"] = df[src_entry] if src_entry and src_entry in df.columns else pd.NaT
        df[f"stage_{n}_exit"]  = df[src_exit]  if src_exit  and src_exit  in df.columns else pd.NaT
        df[f"stage_{n}_days"]  = pd.to_numeric(
            df[src_days] if src_days and src_days in df.columns else pd.Series(np.nan, index=df.index),
            errors="coerce"
        )

    # Drop colunas-fonte das etapas (ex: "Etapa 5", "Data de entrada na etapa 5")
    # após criar as canônicas stage_N_*. Evita duplicar até 80 colunas em memória.
    orig_stage_srcs = {v for cols in stage_cols.values() for v in cols.values() if v}
    df = df.drop(columns=[c for c in orig_stage_srcs if c in df.columns], errors="ignore")
    gc.collect()

    # Atualiza stage_cols para apontar para os nomes CANÔNICOS das colunas (stage_N_*)
    # pois as colunas originais foram dropadas acima.
    # calcular_funil_dinamico e calcular_tempo_dinamico dependem disso para funcionar.
    canonical_stage_cols: dict = {}
    for n in stage_cols.keys():
        entry = {}
        if f"stage_{n}_name"  in df.columns: entry["name"]  = f"stage_{n}_name"
        if f"stage_{n}_entry" in df.columns: entry["entry"] = f"stage_{n}_entry"
        if f"stage_{n}_exit"  in df.columns: entry["exit"]  = f"stage_{n}_exit"
        if f"stage_{n}_days"  in df.columns: entry["days"]  = f"stage_{n}_days"
        if entry:
            canonical_stage_cols[n] = entry
    stage_cols = canonical_stage_cols

    # ── 7. Join DigAI base (email → phone) ────────────────────────────────────
    df["data_ei"]         = pd.NaT
    df["score_ia"]        = np.nan
    df["score_inicial_ia"]= np.nan
    df["aprovado_ia"]     = False
    df["ranking_ia"]      = np.nan
    df["vaga_digai"]      = ""
    df["_in_digai"]       = False   # flag de match com base DigAI

    if digai_df is not None and len(digai_df) > 0:
        digai_cols = ["email", "data_ei", "_in_digai"]
        for c in ("phone", "score_ia", "score_inicial_ia", "aprovado_ia",
                  "ranking_ia", "vaga_digai", "status_ia",
                  "empresa", "score_editado", "req_atendido", "dias_vaga", "workspace",
                  "nome_digai", "estado_digai", "cidade_digai"):
            if c in digai_df.columns:
                digai_cols.append(c)

        # Pré-computa subset para phone join (antes de liberar digai_df)
        _has_phone_col = "phone" in digai_df.columns
        if _has_phone_col:
            digai_phone_full = digai_df[digai_df["phone"].ne("")][
                [c for c in digai_cols if c in digai_df.columns]
            ].drop_duplicates(subset=["phone"]).rename(columns={"email": "email_digai_ph"})
        else:
            digai_phone_full = None

        # Pré-computa subset para CPF join (chave terciária)
        _has_cpf_col = "cpf" in digai_df.columns
        if _has_cpf_col:
            digai_cpf_full = (
                digai_df[digai_df["cpf"].ne("")]
                [[c for c in digai_cols + ["cpf"] if c in digai_df.columns]]
                .drop_duplicates(subset=["cpf"])
                .rename(columns={"email": "email_digai_cpf"})
            )
        else:
            digai_cpf_full = None

        # Salva tamanho total antes de liberar.
        # Se já temos _total_digai_base do DigAIResult (contagem pré-deduplicação),
        # usamos esse valor (mais preciso para ROI). Caso contrário, usa len(digai_df).
        _total_digai = _total_digai_base if _total_digai_base > 0 else len(digai_df)

        digai_sub = digai_df[[c for c in digai_cols if c in digai_df.columns]].copy()
        # Libera digai_df — não é mais necessário (subsets já criados)
        digai_df = None; gc.collect()

        digai_sub = digai_sub[digai_sub["email"].ne("")]

        # Diagnóstico do join
        n_funnel_emails = df["email"].ne("").sum()
        n_digai_emails  = digai_sub["email"].ne("").sum()
        print(f"   🔍 Diagnóstico join: {n_funnel_emails:,} emails no ATS | {n_digai_emails:,} emails na base DigAI")
        if n_funnel_emails > 0:
            print(f"   📧 Amostra ATS:   {df[df['email'].ne('')]['email'].head(3).tolist()}")
        if n_digai_emails > 0:
            print(f"   📧 Amostra DigAI: {digai_sub['email'].head(3).tolist()}")

        # ── Validação pré-join: duplicatas no DigAI inflam linhas ────────────
        n_before = len(df)
        dupes_digai = digai_sub.duplicated("email").sum()
        if dupes_digai > 0:
            print(f"   ⚠️  {dupes_digai} emails duplicados na base DigAI — deduplicando antes do join")
            digai_sub = digai_sub.drop_duplicates("email", keep="first")

        # Join por email
        df = df.merge(digai_sub, on="email", how="left",
                      suffixes=("", "_digai"))

        # ── Assert: LEFT join não deve adicionar linhas ────────────────────
        if len(df) != n_before:
            print(f"   ⚠️  Join alterou número de linhas! {n_before} → {len(df)}")
            print(f"        Causa: emails duplicados remanescentes no DigAI. Forçando deduplicação.")
            df = df.drop_duplicates(subset=["candidato_id"] if "candidato_id" in df.columns else None,
                                    keep="first")

        # Consolida colunas duplicadas
        for col in ("data_ei", "score_ia", "score_inicial_ia", "aprovado_ia",
                    "ranking_ia", "vaga_digai", "_in_digai"):
            if f"{col}_digai" in df.columns:
                if col == "_in_digai":
                    # _in_digai foi inicializado como False (não NaN) antes do merge,
                    # então fillna nunca dispararia. Usamos o lado direito diretamente.
                    df[col] = df[f"{col}_digai"].fillna(False).astype(bool)
                else:
                    df[col] = df[col].fillna(df[f"{col}_digai"])
                df = df.drop(columns=[f"{col}_digai"])

        # Enriquece phone com o da base DigAI onde o ATS não tem
        if "phone_digai" in df.columns:
            df["phone"] = (
                df["phone"].replace("", pd.NA)
                .fillna(df["phone_digai"])
                .fillna("")
                .astype(str)
            )
            df = df.drop(columns=["phone_digai"])

        # Propaga nome da base DigAI onde o funil não tem
        if "nome_digai" in df.columns:
            if "nome" not in df.columns or df["nome"].isna().all() or df["nome"].eq("").all():
                df["nome"] = df["nome_digai"].fillna("")
            else:
                df["nome"] = df["nome"].replace("", pd.NA).fillna(df["nome_digai"]).fillna("")
            df = df.drop(columns=["nome_digai"])

        # Propaga estado e cidade da base DigAI (origem geográfica sem DDD)
        for geo_col in ("estado_digai", "cidade_digai"):
            if geo_col in df.columns:
                # Mantém como coluna — analytics.py usa diretamente
                pass  # não dropa — será usada por calcular_origem_candidatos

        # _in_digai: True onde o join encontrou match
        df["_in_digai"] = df["_in_digai"].fillna(False).astype(bool)

        n_matched_email = df["_in_digai"].sum()
        print(f"   ✅ Join email: {n_matched_email:,} matches")

        # Libera digai_sub — não é mais necessário
        _sample_digai_email = digai_sub["email"].head(3).tolist()
        del digai_sub; gc.collect()

        # ── Diagnóstico quando 0 matches ──────────────────────────────────
        if n_matched_email == 0 and n_funnel_emails > 0:
            print(f"   ❌ Zero matches! Rodando diagnóstico...")
            sample_ats = df[df["email"].ne("")]["email"].head(3).tolist()
            print(f"   📧 Amostra ATS   : {sample_ats}")
            print(f"   📧 Amostra DigAI : {_sample_digai_email}")
            tem_espaco_ats = df["email"].str.contains(r"\s", na=False).sum()
            tem_maiusc_ats = df["email"].str.contains(r"[A-Z]", na=False).sum()
            if tem_espaco_ats > 0:
                print(f"   ⚠️  {tem_espaco_ats} emails do ATS com espaço após normalização — bug de encoding")
            if tem_maiusc_ats > 0:
                print(f"   ⚠️  {tem_maiusc_ats} emails do ATS com maiúscula após normalização — bug de encoding")
            print(f"   💡 Possíveis causas: períodos diferentes, cliente diferente, ou coluna de email errada")

        # Join por phone (para candidatos sem email match)
        unmatched_mask = (~df["_in_digai"]) & df["phone"].ne("")
        if unmatched_mask.any() and digai_phone_full is not None:
            phone_match = (
                df[unmatched_mask][["phone"]]
                .reset_index(drop=True)
                .merge(digai_phone_full, on="phone", how="left")
            )
            for col in ("data_ei", "score_ia", "score_inicial_ia",
                        "aprovado_ia", "ranking_ia", "vaga_digai", "_in_digai"):
                if col in phone_match.columns and len(phone_match) == unmatched_mask.sum():
                    df[col] = df[col].astype(phone_match[col].dtype)
                    df.loc[unmatched_mask, col] = phone_match[col].values
            df["_in_digai"] = df["_in_digai"].fillna(False).infer_objects(copy=False).astype(bool)
            n_matched_phone = df["_in_digai"].sum() - n_matched_email
            print(f"   ✅ Join phone: {n_matched_phone:,} matches adicionais")
            del digai_phone_full; gc.collect()

        # Join por CPF (chave terciária — para candidatos sem email nem phone match)
        _cpf_series = df["cpf"] if "cpf" in df.columns else pd.Series("", index=df.index)
        if isinstance(_cpf_series, pd.DataFrame): _cpf_series = _cpf_series.iloc[:, 0]
        unmatched_cpf = (~df["_in_digai"]) & _cpf_series.ne("")
        n_total_matched_before_cpf = int(df["_in_digai"].sum())
        if unmatched_cpf.any() and digai_cpf_full is not None and "cpf" in df.columns:
            cpf_match = (
                df[unmatched_cpf][["cpf"]]
                .reset_index(drop=True)
                .merge(digai_cpf_full, on="cpf", how="left")
            )
            for col in ("data_ei", "score_ia", "score_inicial_ia",
                        "aprovado_ia", "ranking_ia", "vaga_digai", "_in_digai"):
                if col in cpf_match.columns and len(cpf_match) == unmatched_cpf.sum():
                    df.loc[unmatched_cpf, col] = cpf_match[col].values
            df["_in_digai"] = df["_in_digai"].fillna(False).infer_objects(copy=False).astype(bool)
            n_matched_cpf = int(df["_in_digai"].sum()) - n_total_matched_before_cpf
            print(f"   ✅ Join CPF: {n_matched_cpf:,} matches adicionais")
        if digai_cpf_full is not None:
            del digai_cpf_full; gc.collect()

    else:
        _total_digai = _total_digai_base  # 0 quando digai_df não foi fornecido

    # Armazena total da base DigAI para cálculos de assertividade/ROI
    df.attrs["total_digai_base"] = _total_digai

    # ── 8. Segmentação Com / Sem DigAI ────────────────────────────────────────
    # REGRA: quando a base DigAI é fornecida, a segmentação vem EXCLUSIVAMENTE
    # do chaveamento email/phone com essa base (_in_digai = True/False).
    #
    # A etapa "Entrevista Inteligente" do ATS (ei_stage_col) NÃO deve ser usada
    # como critério de segmentação — em muitos clientes TODOS os candidatos passam
    # por essa etapa no ATS independente de terem usado a triagem DigAI de fato.
    # Usar data_ei.notna() como OR inflaria 100% para "Com DigAI".
    #
    # ei_stage_col é usado SOMENTE para enriquecer data_ei (para cálculos de tempo),
    # nunca para determinar a segmentação.
    if _total_digai > 0:
        com_mask = df["_in_digai"].astype(bool)
        strategy = "DigAI base (email/phone match exclusivo)"
        n_com = int(com_mask.sum())
        if n_com == 0:
            print("   ⚠️  0 matches com a base DigAI — verificar formato de email/CPF em ambos os arquivos")
            print(f"   💡 ATS emails (amostra): {df[df['email'].ne('')]['email'].head(3).tolist()}")
        elif n_com == len(df):
            print("   ⚠️  100% dos candidatos matched — base DigAI pode ter emails do ATS completo, não filtrada por cliente/período")
    elif _is_gupy_candidature and ei_stage_col and ei_stage_col in df.columns:
        # Arquivo enviado como "base DigAI" era um export Gupy Candidaturas.
        # Usa a etapa EI do ATS como fallback de segmentação.
        com_mask = df[ei_stage_col].notna()
        strategy = "ATS EI stage (arquivo DigAI era export Gupy — fallback automático)"
    elif ei_stage_col and ei_stage_col in df.columns:
        # Fallback: base DigAI NÃO fornecida → usa etapa EI do ATS como proxy
        com_mask = df[ei_stage_col].notna()
        strategy = "ATS EI stage (fallback — base DigAI não fornecida)"
    else:
        com_mask = pd.Series(False, index=df.index)
        strategy = "nenhuma (sem base DigAI e sem etapa EI no ATS)"

    # Enriquece data_ei com a data do ATS apenas DEPOIS da segmentação
    # (para cálculos de tempo por etapa, não para segmentação)
    if ei_stage_col and ei_stage_col in df.columns:
        df["data_ei"] = df["data_ei"].fillna(df[ei_stage_col])

    df["processo_seletivo"] = SEM_DIGAI
    df.loc[com_mask, "processo_seletivo"] = COM_DIGAI
    df["processo_seletivo"] = df["processo_seletivo"].astype("category")

    # Coluna legível para o Excel: "DigAI Realizado" com SIM/NÃO
    # Usa com_mask (não _in_digai) para ser consistente com a estratégia de segmentação
    df["digai_realizado"] = com_mask.map({True: "SIM", False: "NÃO"})

    n_com = int(com_mask.sum())
    n_sem = len(df) - n_com
    print(f"   🏷️  Segmentação ({strategy}): {n_com:,} Com DigAI | {n_sem:,} Sem DigAI")

    # ── 9. candidato_id ───────────────────────────────────────────────────────
    if "candidato_id" not in df.columns:
        df["candidato_id"] = [f"CAND-{i+1:06d}" for i in range(len(df))]
    else:
        df["candidato_id"] = df["candidato_id"].fillna(
            pd.Series([f"CAND-{i+1:06d}" for i in range(len(df))], index=df.index)
        )

    # ── 10. nome e vaga ───────────────────────────────────────────────────────
    if "nome" not in df.columns:
        df["nome"] = ""
    if "vaga" not in df.columns:
        if "vaga_cand" in df.columns:
            df["vaga"] = df["vaga_cand"] if not isinstance(df["vaga_cand"], pd.DataFrame) else df["vaga_cand"].iloc[:, 0]
        elif "vaga_digai" in df.columns:
            df["vaga"] = df["vaga_digai"] if not isinstance(df["vaga_digai"], pd.DataFrame) else df["vaga_digai"].iloc[:, 0]
        else:
            df["vaga"] = ""

    # ── 11. Limpeza final ─────────────────────────────────────────────────────
    # Retira colunas raw/temporárias
    drop_cols = [c for c in df.columns if c.endswith(("_raw", "_cand", "_gupy",
                                                        "_funnel", "_digai_ph"))
                 and c not in ("vaga_digai",)]
    # Mantém colunas importantes mesmo que terminem em _gupy por coincidência
    keep = {"data_final_gupy", "status_gupy", "data_contratacao_gupy",
            "data_cadastro_funnel", "data_inscricao_funnel"}
    drop_cols = [c for c in drop_cols if c not in keep]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # ── 12. Enriquecimento demográfico ───────────────────────────────────────
    try:
        from .enrichment import enrich_dataframe
        enrich_dataframe(df)
    except Exception as _e:
        print(f"   ⚠️  Enriquecimento demográfico ignorado: {_e}")

    # Mantém attrs para compatibilidade com código legado que lê df.attrs diretamente
    df.attrs["stage_cols"]      = stage_cols
    df.attrs["ei_stage_col"]    = ei_stage_col
    df.attrs["strategy"]        = strategy
    df.attrs["n_stages"]        = len(stage_cols)
    df.attrs["total_digai_base"] = _total_digai

    print(f"   ✅ Unified: {len(df):,} candidatos | {len(df.columns)} colunas")

    # Retorna SegmentationResult com metadados explícitos (não dependem de df.attrs)
    result = SegmentationResult(
        df=df,
        strategy=strategy,
        stage_cols=stage_cols,
        ei_stage_col=ei_stage_col,
        dims_detected=_dims_detected,
        total_digai_base=_total_digai,
    )
    return result
