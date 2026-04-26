"""
DigAI Reports Engine — Orquestrador Central

Responsabilidade única: sequenciar as etapas do pipeline com logging por step,
checkpoints de validação explícitos e propagação limpa de erros.

Substitui a orquestração dispersa em analytics.gerar_relatorio_from_sources()
e no handler HTTP app.py:/gerar.

Uso:
    from engine.pipeline import run

    relatorio, df = run(
        funnel_path="funnel.csv",
        candidatura_path="candidatura.csv",
        digai_path="digai.csv",
        params={"cliente_nome": "Atento", "mensalidade_digai": 7600},
        session_id="abc123",   # opcional, para logs correlacionados
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
        self.step    = step
        self.fatais  = fatais or []
        super().__init__(f"[{step}] {message}")


def _log(session_id: str | None, step: str, msg: str, elapsed: float | None = None):
    prefix = f"[{session_id}]" if session_id else "[pipeline]"
    suffix = f" ({elapsed:.1f}s)" if elapsed is not None else ""
    print(f"{prefix} step={step} {msg}{suffix}", flush=True)


def run(
    funnel_path: str,
    candidatura_path: Optional[str],
    digai_path: Optional[str],
    params: dict | None = None,
    session_id: str | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Executa o pipeline completo: ingestion → segmentation → analytics → (retorna).

    Raises
    ------
    PipelineError
        Se qualquer etapa produzir um alerta fatal (ex: 0 candidatos Com DigAI,
        DataFrame vazio após segmentação).

    Returns
    -------
    (relatorio_dict, df)
        relatorio_dict : dict com todos os KPIs, ROI, funil, insights, etc.
                         NÃO contém "_df" — DataFrame retornado separadamente.
        df             : DataFrame unificado (para geração de Excel e diagnósticos).
    """
    from .ingestion import load_gupy_funnel, load_gupy_candidatura, load_digai_base
    from .segmentation import build_unified
    from .analytics import (
        calcular_kpis, calcular_roi, calcular_funil, calcular_funil_dinamico,
        calcular_tempo_por_etapa, calcular_tempo_dinamico, calcular_status,
        gerar_insights, gerar_narrativa, calcular_mapa_vagas,
        calcular_periodo_comparativo, calcular_origem_candidatos,
        diagnostico_qualidade,
    )
    from .enrichment import calcular_perfil_aprovados
    from datetime import datetime

    params = params or {}
    t0_total = time.time()

    # ── Step 1: Ingestion ──────────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "ingestion", "start")
    try:
        funnel_result     = load_gupy_funnel(funnel_path)
        candidatura_result = load_gupy_candidatura(candidatura_path) if candidatura_path else None
        digai_result       = load_digai_base(digai_path) if digai_path else None
    except Exception as e:
        raise PipelineError("ingestion", str(e)) from e

    _log(session_id, "ingestion", "done", elapsed=time.time() - t0)

    # Validação pós-ingestion
    if funnel_result.df.empty:
        raise PipelineError(
            "ingestion",
            "DataFrame do funil está vazio após carregamento. "
            "Verifique se o arquivo é válido e não está corrompido.",
        )
    if not funnel_result.has_emails and (digai_result is None or digai_result.df.empty):
        raise PipelineError(
            "ingestion",
            "Funil sem emails e sem base DigAI — join impossível. "
            "Forneça pelo menos um dos dois.",
        )

    # ── Step 2: Segmentation ───────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "segmentation", "start",)
    try:
        seg_result = build_unified(funnel_result, candidatura_result, digai_result)
    except Exception as e:
        raise PipelineError("segmentation", str(e)) from e
    finally:
        del funnel_result, candidatura_result, digai_result
        gc.collect()

    _log(session_id, "segmentation", "done", elapsed=time.time() - t0)

    # Validação pós-segmentação via SegmentationResult.validate()
    seg_errors = seg_result.validate()
    fatais  = [e for e in seg_errors if e.startswith("❌")]
    if fatais:
        raise PipelineError(
            "segmentation",
            "Dado crítico detectado:\n" + "\n".join(fatais),
            fatais=fatais,
        )

    # Desempacota DataFrame e propaga metadados via df.attrs (compatibilidade legada)
    df = seg_result.df
    df.attrs["stage_cols"]       = seg_result.stage_cols
    df.attrs["ei_stage_col"]     = seg_result.ei_stage_col
    df.attrs["strategy"]         = seg_result.strategy
    df.attrs["n_stages"]         = len(seg_result.stage_cols)
    df.attrs["total_digai_base"] = seg_result.total_digai_base
    del seg_result
    gc.collect()

    # ── Step 3: Analytics ─────────────────────────────────────────────────────
    t0 = time.time()
    _log(session_id, "analytics", "start")
    try:
        alertas_qualidade = diagnostico_qualidade(df)
        for a in alertas_qualidade:
            _log(session_id, "analytics", a)

        kpis              = calcular_kpis(df)
        roi               = calcular_roi(df, params)
        funil             = calcular_funil(df)
        funil_din         = calcular_funil_dinamico(df)
        tempos            = calcular_tempo_por_etapa(df)
        tempos_din        = calcular_tempo_dinamico(df)
        status            = calcular_status(df)
        insights          = gerar_insights(kpis, roi)
        mapa_vagas        = calcular_mapa_vagas(df)
        periodo_comp      = calcular_periodo_comparativo(df)
        origem_candidatos = calcular_origem_candidatos(df)

        perfil_aprovados  = calcular_perfil_aprovados(df)

        meta = {
            "cliente":   params.get("cliente_nome", "Cliente"),
            "periodo":   params.get("periodo", ""),
            "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "logo_url":  params.get("logo_url", ""),
            "strategy":  df.attrs.get("strategy", ""),
        }
        narrativa = gerar_narrativa(kpis, roi, meta)

    except PipelineError:
        raise
    except Exception as e:
        raise PipelineError("analytics", str(e)) from e

    _log(session_id, "analytics", "done", elapsed=time.time() - t0)

    relatorio = {
        "meta":                meta,
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
        # "_df" é retornado separadamente — não embutido no dict
    }

    _log(session_id, "pipeline", "complete", elapsed=time.time() - t0_total)
    return relatorio, df
