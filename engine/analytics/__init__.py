"""
engine/analytics — Pacote de cálculo de métricas DigAI

Re-exporta todas as funções públicas de _analytics_core.py para manter
100% de compatibilidade com imports existentes:

    from engine.analytics import calcular_kpis, calcular_roi, ...

O arquivo engine/_analytics_core.py contém o código canônico.
Este __init__.py atua como fachada de re-export.

Estrutura interna (para referência e futuras extrações):
  constants.py           — constantes compartilhadas (DEFAULTS, ETAPAS, etc.)
  _analytics_core.py     — implementação completa (fonte canônica atual)
"""

from engine._analytics_core import (  # noqa: F401
    # Constantes
    DEFAULTS,
    ETAPAS,
    STATUS_CONTRATADO,
    STATUS_DESISTIU,
    STATUS_REPROVADO,
    STATUS_EM_PROCESSO,
    COM_DIGAI,
    SEM_DIGAI,

    # Carregamento legado
    load_data,

    # KPIs
    calcular_kpis,

    # ROI
    calcular_roi,

    # Origem geográfica
    calcular_origem_candidatos,

    # Funil de conversão
    calcular_funil,
    calcular_funil_dinamico,

    # Tempo por etapa
    calcular_tempo_por_etapa,
    calcular_tempo_dinamico,

    # Status dos candidatos
    calcular_status,
    calcular_assertividade_ia,
    calcular_area_negocio,

    # Qualidade de dados
    diagnostico_qualidade,
    analisar_qualidade,

    # Insights e veredicto
    gerar_insights,

    # Narrativa e blocos adicionais
    gerar_narrativa,
    calcular_mapa_vagas,
    calcular_periodo_comparativo,

    # Entry points do pipeline (mantidos para compatibilidade)
    gerar_relatorio,
    gerar_relatorio_from_sources,
)

__all__ = [
    "DEFAULTS", "ETAPAS",
    "STATUS_CONTRATADO", "STATUS_DESISTIU", "STATUS_REPROVADO", "STATUS_EM_PROCESSO",
    "COM_DIGAI", "SEM_DIGAI",
    "load_data",
    "calcular_kpis",
    "calcular_roi",
    "calcular_origem_candidatos",
    "calcular_funil", "calcular_funil_dinamico",
    "calcular_tempo_por_etapa", "calcular_tempo_dinamico",
    "calcular_status", "calcular_assertividade_ia", "calcular_area_negocio",
    "diagnostico_qualidade", "analisar_qualidade",
    "gerar_insights",
    "gerar_narrativa", "calcular_mapa_vagas", "calcular_periodo_comparativo",
    "gerar_relatorio", "gerar_relatorio_from_sources",
]
