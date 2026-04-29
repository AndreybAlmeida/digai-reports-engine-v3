"""
DigAI Reports Engine — Orquestrador Central (v3 — motor flexível)

Arquitetura de 4 cenários:
  Cenário 1 — DigAI + Step Funnel completo (ATS com etapas)
  Cenário 2 — DigAI + base de contratações (sem etapas)
  Cenário 3 — apenas base DigAI
  Cenário 4 — DigAI + ATS parcial (sem etapas ou etapas incompletas)

Regra mestra: a base DigAI é SEMPRE obrigatória e central.
O funil do ATS é enriquecimento — nunca requisito.

Uso:
    from engine.pipeline import run

    relatorio, df = run(
        digai_path="digai.csv",         # obrigatório
        funnel_path="funnel.csv",        # opcional
        candidatura_path="cand.csv",     # opcional
        params={"cliente_nome": "Atento", "mensalidade_digai": 7600},
        session_id="abc123",
    )
"""

from __future__ import annotations

import gc
import time
from typing import Optional

import pandas as pd


class PipelineError(Exception):
    """
    Erro explícito do pipeline — contém o step onde ocorreu e
    a lista de alertas fatais que motivaram o abort.
    """
    def __init__(self, step: str, message: str, fatais: list[str] | None = None):
        self.step   = step
        self.fatais = fatais or []
        super().__init__(f"[{step}] {message}")


def _log(session_id: str | None, step: str, msg: str, elapsed: float | None = None):
    prefix = f"[{session_id}]" if session_id else "[pipeline]"
    suffix = f" ({elapsed:.1f}s)" if elapsed is not None else ""
    print(f"{prefix} step={step} {msg}{suffix}", flush=True)


def _safe_analytics(fn, *args, reason: str = "", session_id=None, **kwargs):
    """
    Chama uma função de analytics com fallback seguro.
    Retorna {"_unavailable": reason} se a função lançar exceção.
    Nunca deixa o pipeline quebrar por dados ausentes.
    """
    try:
        result = fn(*args, **kwargs)
        # Garante que listas/dicts vazios não quebrem o dashboard
        if result is None:
            return {"_unavailable": reason or "Dado não disponível"}
        return result
    except Exception as e:
        _log(session_id, "analytics", f"⚠️ {fn.__name__} indisponível: {e}")
        return {"_unavailable": reason or str(e)}


def _enrich_hired_from_candidatura(df: "pd.DataFrame", cand_result) -> None:
    """
    Cenário 2: marca como Contratado na base DigAI-only os emails presentes
    na base de candidaturas/contratações. Modifica df in-place.
    """
    import pandas as _pd
    from .schema import CandidaturaResult

    cand_df = cand_result.df if isinstance(cand_result, CandidaturaResult) else cand_result
    if cand_df is None or cand_df.empty:
        return

    # Determina emails de contratados na base candidatura
    _status_col = next((c for c in ("status_cand", "status") if c in cand_df.columns), None)
    _date_col   = next((c for c in ("data_contratacao_cand", "data_contratacao") if c in cand_df.columns), None)
    _email_col  = "email" if "email" in cand_df.columns else None

    if not _email_col:
        return

    hired_emails: set = set()
    if _status_col:
        mask = cand_df[_status_col].astype(str).str.lower().str.contains("contrat|hired", na=False)
        hired_emails |= set(cand_df.loc[mask, _email_col].dropna().astype(str).str.strip().str.lower())
    if _date_col:
        mask_date = cand_df[_date_col].notna()
        hired_emails |= set(cand_df.loc[mask_date, _email_col].dropna().astype(str).str.strip().str.lower())

    if not hired_emails or "email" not in df.columns:
        return

    email_col_lower = df["email"].astype(str).str.strip().str.lower()
    mask_hired = email_col_lower.isin(hired_emails)
    df.loc[mask_hired, "status"] = "Contratado"

    # Propaga data_contratacao se disponível
    if _date_col and "data_contratacao" in df.columns:
        cand_map = cand_df.set_index(_email_col)[_date_col].to_dict()
        df.loc[mask_hired, "data_contratacao"] = (
            email_col_lower[mask_hired].map(cand_map)
        )
        df["data_contratacao"] = _pd.to_datetime(df["data_contratacao"], errors="coerce")

    n = int(mask_hired.sum())
    print(f"   ✅ Cenário 2: {n:,} contratados identificados via base de candidaturas", flush=True)


def _build_capabilities(
    funnel_path: Optional[str],
    candidatura_path: Optional[str],
    seg_result,
    df: pd.DataFrame,
) -> "DataCapabilities":
    """
    Detecta o cenário e constrói DataCapabilities após a segmentação.
    """
    from .schema import DataCapabilities, _MSG

    has_funnel      = funnel_path is not None
    has_candidatura = candidatura_path is not None
    has_stage_cols  = bool(seg_result.stage_cols)
    digai_only      = seg_result.digai_only
    n_com           = seg_result.n_com_digai
    n_sem           = seg_result.n_sem_digai
    has_comparison  = n_sem > 0

    has_hired = False
    if "status" in df.columns:
        has_hired = (df["status"] == "Contratado").any()

    has_dates = False
    if "data_cadastro" in df.columns:
        has_dates = df["data_cadastro"].notna().any()

    # Determina cenário
    if digai_only or (not has_funnel and not has_candidatura):
        scenario = "digai_only"
    elif not has_funnel and has_candidatura:
        scenario = "digai_hired"
    elif has_funnel and has_stage_cols:
        scenario = "digai_ats_full"
    else:
        scenario = "digai_ats_partial"

    can_calc_funil        = has_funnel and has_stage_cols
    can_calc_sla          = has_funnel and has_stage_cols and has_dates
    can_calc_assertividade = has_funnel and has_stage_cols and has_comparison and n_com > 0
    can_compare_groups    = has_comparison
    can_calc_conversion   = has_funnel and has_stage_cols

    unavailable: dict[str, str] = {}
    if not can_calc_funil:
        unavailable["funil"] = _MSG["funil"]
    if not can_calc_sla:
        unavailable["sla"] = _MSG["sla"]
    if not can_calc_assertividade:
        unavailable["assertividade"] = _MSG["assertividade"]
    if not can_compare_groups:
        unavailable["comparativo"] = _MSG["comparativo"]
    if not can_calc_conversion:
        unavailable["timeline"] = _MSG["timeline"]
    if not has_hired and not has_funnel:
        unavailable["contratacoes"] = _MSG["contratacoes"]

    return DataCapabilities(
        scenario              = scenario,
        has_funnel            = has_funnel,
        has_candidatura       = has_candidatura,
        has_stage_cols        = has_stage_cols,
        has_comparison_group  = has_comparison,
        can_calc_funil        = can_calc_funil,
        can_calc_sla          = can_calc_sla,
        can_calc_assertividade = can_calc_assertividade,
        can_calc_roi          = True,
        can_calc_saving       = True,
        can_compare_groups    = can_compare_groups,
        can_calc_conversion   = can_calc_conversion,
        can_calc_hired        = has_hired,
        unavailable           = unavailable,
    )


def run(
    digai_path: str,
    funnel_path: Optional[str] = None,
    candidatura_path: Optional[str] = None,
    params: dict | None = None,
    session_id: str | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Executa o pipeline completo: ingestion → segmentation → analytics → (retorna).

    A base DigAI é SEMPRE obrigatória. O funil do ATS e a base de candidaturas
    são opcionais — o relatório é gerado mesmo sem eles.

    Parâmetros
    ----------
    digai_path       : caminho para a base DigAI (obrigatório)
    funnel_path      : caminho para o relatório de etapas do ATS (opcional)
    candidatura_path : caminho para a base de candidaturas/contratações (opcional)
    params           : parâmetros do relatório (cliente, mensalidade, etc.)
    session_id       : identificador de sessão para logs correlacionados

    Retorna
    -------
    (relatorio_dict, df)
        relatorio_dict : dict com todos os KPIs disponíveis + 'capabilities' descrevendo
                         o que está disponível e o motivo de cada KPI indisponível.
        df             : DataFrame unificado (para Excel e diagnósticos).

    Raises
    ------
    PipelineError
        Apenas em situações verdadeiramente fatais (base DigAI vazia, 0 candidatos Com DigAI
        quando deveria haver, etc.). Não lança exceção por KPI indisponível.
    """
    from .ingestion import load_gupy_funnel, load_gupy_candidatura, load_digai_base
    from .segmentation import build_unified, build_digai_only
    from .analytics import (
        calcular_kpis, calcular_roi, calcular_funil, calcular_funil_dinamico,
        calcular_tempo_por_etapa, calcular_tempo_dinamico, calcular_status,
        gerar_insights, gerar_narrativa, calcular_mapa_vagas,
        calcular_periodo_comparativo, calcular_origem_candidatos,
        diagnostico_qualidade, calcular_assertividade_ia, calcular_area_negocio,
    )
    from .enrichment import calcular_perfil_aprovados
    from .schema import DataCapabilities, _MSG
    from datetime import datetime

    params = params or {}
    t0_total = time.time()

    # ── Step 1: Ingestion ──────────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "ingestion", "start")

    scenario_label = "digai_only"
    if funnel_path:
        scenario_label = "digai_ats"
    elif candidatura_path:
        scenario_label = "digai_hired"
    _log(session_id, "ingestion", f"cenário detectado: {scenario_label}")

    try:
        digai_result = load_digai_base(digai_path)
    except Exception as e:
        raise PipelineError("ingestion", f"Falha ao carregar base DigAI: {e}") from e

    if digai_result.df.empty:
        raise PipelineError(
            "ingestion",
            "Base DigAI está vazia após carregamento. "
            "Verifique se o arquivo é válido e não está corrompido.",
        )

    funnel_result      = None
    candidatura_result = None

    if funnel_path:
        try:
            funnel_result = load_gupy_funnel(funnel_path)
        except Exception as e:
            _log(session_id, "ingestion", f"⚠️ Falha ao carregar funil ATS: {e} — continuando sem funil")
            funnel_result = None

    if candidatura_path:
        try:
            candidatura_result = load_gupy_candidatura(candidatura_path)
        except Exception as e:
            _log(session_id, "ingestion", f"⚠️ Falha ao carregar candidaturas: {e} — continuando sem candidaturas")
            candidatura_result = None

    _log(session_id, "ingestion", "done", elapsed=time.time() - t0)

    # ── Step 2: Segmentation ───────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "segmentation", "start")

    try:
        if funnel_result is not None:
            # Cenário 1 ou 4: ATS disponível (com ou sem candidatura)
            seg_result = build_unified(funnel_result, candidatura_result, digai_result)
        else:
            # Sem Step Funnel — base DigAI é a fonte primária
            _log(session_id, "segmentation", "DigAI-only (sem Step Funnel)")
            seg_result = build_digai_only(digai_result)

            # Cenário 2: enriquece com status de contratados da base de candidaturas
            if candidatura_result is not None:
                _log(session_id, "segmentation", "enriquecendo com base de contratações")
                _enrich_hired_from_candidatura(seg_result.df, candidatura_result)
    except Exception as e:
        raise PipelineError("segmentation", str(e)) from e
    finally:
        del funnel_result, candidatura_result, digai_result
        gc.collect()

    _log(session_id, "segmentation", "done", elapsed=time.time() - t0)

    # Validação pós-segmentação
    seg_errors = seg_result.validate()
    fatais = [e for e in seg_errors if e.startswith("❌")]
    if fatais:
        raise PipelineError(
            "segmentation",
            "Dado crítico detectado:\n" + "\n".join(fatais),
            fatais=fatais,
        )

    # Desempacota DataFrame e propaga metadados
    df = seg_result.df
    df.attrs["stage_cols"]       = seg_result.stage_cols
    df.attrs["ei_stage_col"]     = seg_result.ei_stage_col
    df.attrs["strategy"]         = seg_result.strategy
    df.attrs["n_stages"]         = len(seg_result.stage_cols)
    df.attrs["total_digai_base"] = seg_result.total_digai_base

    # Constrói capabilities ANTES de deletar seg_result
    caps = _build_capabilities(funnel_path, candidatura_path, seg_result, df)
    del seg_result
    gc.collect()

    # ── Step 3: Analytics ─────────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "analytics", f"start (cenário: {caps.scenario})")

    _ua = caps.unavailable  # alias curto

    def _safe(fn, *args, key="", **kwargs):
        return _safe_analytics(fn, *args, reason=_ua.get(key, ""), session_id=session_id, **kwargs)

    try:
        alertas_qualidade = diagnostico_qualidade(df)
        for a in alertas_qualidade:
            _log(session_id, "analytics", a)

        kpis = _safe(calcular_kpis, df, key="comparativo")

        roi  = _safe(calcular_roi, df, params, key="")

        # Funil fixo (legacy) — apenas quando há dados de status
        funil = _safe(calcular_funil, df, key="funil") if caps.has_comparison_group else {"_unavailable": _ua.get("funil", _MSG["funil"])}

        # Funil dinâmico (por etapas ATS)
        funil_din = _safe(calcular_funil_dinamico, df, key="funil") if caps.can_calc_funil else {"_unavailable": _ua.get("funil", _MSG["funil"])}

        # SLA
        tempos     = _safe(calcular_tempo_por_etapa, df, key="sla") if caps.can_calc_sla else {"_unavailable": _ua.get("sla", _MSG["sla"])}
        tempos_din = _safe(calcular_tempo_dinamico,  df, key="sla") if caps.can_calc_sla else {"_unavailable": _ua.get("sla", _MSG["sla"])}

        status = _safe(calcular_status, df, key="")

        insights  = _safe(gerar_insights, kpis, roi, key="")
        mapa_vagas = _safe(calcular_mapa_vagas, df, key="")
        periodo_comp = _safe(calcular_periodo_comparativo, df, key="")
        origem_candidatos = _safe(calcular_origem_candidatos, df, key="")
        perfil_aprovados  = _safe(calcular_perfil_aprovados, df, key="")
        assertividade_ia  = _safe(calcular_assertividade_ia, df, key="")
        area_negocio      = _safe(calcular_area_negocio, df, key="")

        meta = {
            "cliente":   params.get("cliente_nome", "Cliente"),
            "periodo":   params.get("periodo", ""),
            "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "logo_url":  params.get("logo_url", ""),
            "strategy":  df.attrs.get("strategy", ""),
            "scenario":  caps.scenario,
        }
        narrativa = _safe(gerar_narrativa, kpis, roi, meta, key="")

    except PipelineError:
        raise
    except Exception as e:
        raise PipelineError("analytics", str(e)) from e

    _log(session_id, "analytics", "done", elapsed=time.time() - t0)

    relatorio = {
        "meta":                meta,
        "capabilities":        caps.to_dict(),
        "kpis":                kpis,
        "roi":                 roi,
        "funil":               funil,
        "funil_din":           funil_din,
        "tempos":              tempos,
        "tempos_din":          tempos_din,
        "status":              status,
        "insights":            insights,
        "alertas_qualidade":   alertas_qualidade,
        "narrativa":           narrativa,
        "mapa_vagas":          mapa_vagas,
        "periodo_comparativo": periodo_comp,
        "origem_candidatos":   origem_candidatos,
        "perfil_aprovados":    perfil_aprovados,
        "assertividade_ia":    assertividade_ia,
        "area_negocio":        area_negocio or {"por_area": [], "por_workspace": [], "total": 0},
    }

    _log(session_id, "pipeline", "complete", elapsed=time.time() - t0_total)
    return relatorio, df
