"""
DigAI Reports Engine — Contratos de dados entre módulos

Substitui o padrão frágil de df.attrs como veículo de metadados.
Cada dataclass representa o output de uma etapa do pipeline:

  load_pipeline()   → IngestionResult
  build_unified()   → SegmentationResult
  gerar_relatorio() → é representado pelo dict retornado (mantido por compatibilidade)

Esses tipos garantem que metadados críticos (stage_cols, ei_stage_col, strategy)
nunca se percam silenciosamente em operações pandas que descartam df.attrs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

# Mensagens padrão de insuficiência de dados (exibidas no dashboard)
_MSG = {
    "funil":         "Para calcular o funil detalhado é necessário um relatório de etapas do processo seletivo (Step Funnel ou equivalente).",
    "sla":           "Para calcular SLA é necessário receber uma base com data de entrada, movimentações entre etapas e data de contratação.",
    "assertividade": "Para calcular assertividade é necessário receber dados de avanço, aprovação ou contratação por etapa.",
    "comparativo":   "Indicador comparativo indisponível: a base enviada não contém candidatos sem DigAI para comparação.",
    "timeline":      "Análise de SLA por etapa requer datas de movimentação no ATS.",
    "contratacoes":  "Para identificar contratações é necessário o relatório de contratações ou o Step Funnel com status por etapa.",
}


@dataclass
class IngestionResult:
    """
    Output de load_pipeline() / load_gupy_funnel().
    Carrega o DataFrame do funil + metadados detectados semanticamente.
    """
    df: pd.DataFrame

    # Mapeamento de etapas dinâmicas: {n: {"name": col, "entry": col, ...}}
    stage_cols: dict = field(default_factory=dict)

    # Coluna de entrada da EI no ATS (ou None se não detectada)
    ei_stage_col: Optional[str] = None

    # Dimensões disponíveis: {"area": {"col": "area", "values": [...]}, ...}
    dims_detected: dict = field(default_factory=dict)

    @property
    def n_stages(self) -> int:
        return len(self.stage_cols)

    @property
    def has_emails(self) -> bool:
        return "email" in self.df.columns and self.df["email"].ne("").any()


@dataclass
class CandidaturaResult:
    """
    Output de load_gupy_candidatura() / load_contratacoes().
    Pode ser None quando o arquivo complementar não é fornecido.

    is_contratados=True indica que TODOS os registros são contratados confirmados
    (ex: Relatório de Contratações do Gupy). Quando True, segmentation.py usa este
    arquivo como âncora definitiva: qualquer candidato do funil que NÃO esteja aqui
    não pode ser marcado como Contratado por inferência.
    """
    df: pd.DataFrame
    is_contratados: bool = False  # True = âncora definitiva; False = lista mista


@dataclass
class DigAIResult:
    """
    Output de load_digai_base().
    Inclui o total de entrevistas para cálculo de ROI.
    """
    df: pd.DataFrame
    total: int = 0  # total de entrevistas na base (antes de deduplicação por email)

    # True quando o arquivo detectado é um export do Gupy Candidaturas, não da
    # plataforma DigAI. Nesse caso segmentation.py ignora o email match e usa
    # o ei_stage_col do ATS como fallback de segmentação.
    is_gupy_candidature: bool = False


@dataclass
class SegmentationResult:
    """
    Output de build_unified().
    DataFrame unificado com metadados da segmentação.
    """
    df: pd.DataFrame

    # Estratégia de segmentação usada
    strategy: str = ""

    # Colunas de etapas (preservadas após os merges)
    stage_cols: dict = field(default_factory=dict)
    ei_stage_col: Optional[str] = None
    dims_detected: dict = field(default_factory=dict)

    # Total de entrevistas DigAI (para ROI)
    total_digai_base: int = 0

    @property
    def n_com_digai(self) -> int:
        if "processo_seletivo" not in self.df.columns:
            return 0
        return int((self.df["processo_seletivo"] == "Com DigAI").sum())

    @property
    def n_sem_digai(self) -> int:
        if "processo_seletivo" not in self.df.columns:
            return 0
        return int((self.df["processo_seletivo"] == "Sem DigAI").sum())

    # True quando o relatório foi gerado só com a base DigAI (sem ATS)
    digai_only: bool = False

    def validate(self) -> list[str]:
        """
        Retorna lista de erros e avisos do resultado de segmentação.
        Prefixo '❌' = fatal (pipeline deve abortar).
        Prefixo '⚠️' = aviso (pipeline pode continuar).
        """
        errors: list[str] = []
        if self.df.empty:
            errors.append("❌ CRÍTICO: DataFrame vazio após segmentação.")
            return errors

        n_total = len(self.df)

        # ── Segmentação Com/Sem DigAI ─────────────────────────────────────────
        if self.n_com_digai == 0:
            # 0 Com DigAI = o cruzamento de emails não encontrou matches.
            # Rebaixado para WARNING: o relatório ainda é gerado (todos Sem DigAI),
            # mas o dashboard mostrará o alerta de qualidade.
            # Não bloquear: arquivos podem ser de períodos/clientes diferentes
            # ou ter encoding de email diferente — o usuário deve revisar.
            errors.append(
                f"⚠️ Nenhum candidato identificado como Com DigAI "
                f"(estratégia: {self.strategy or 'desconhecida'}). "
                "Verifique se os arquivos são do mesmo período e cliente "
                "e se os emails do funil coincidem com os da base DigAI. "
                "O relatório foi gerado mas pode estar incompleto."
            )
        if self.n_sem_digai == 0 and not self.digai_only:
            errors.append(
                "⚠️ Todos os candidatos estão no grupo Com DigAI — sem grupo de controle."
            )
        ratio_com = self.n_com_digai / n_total if n_total > 0 else 0
        if ratio_com > 0.99 and n_total > 10 and not self.digai_only:
            errors.append(
                f"⚠️ {ratio_com:.0%} dos candidatos estão em Com DigAI — "
                "verifique se a base DigAI está filtrada pelo período e cliente corretos."
            )

        # ── Contratados ───────────────────────────────────────────────────────
        if "status" in self.df.columns:
            n_contratados = int((self.df["status"] == "Contratado").sum())
            if n_contratados == 0:
                errors.append(
                    "⚠️ Nenhum candidato com status Contratado detectado. "
                    "Verifique o arquivo de contratados ou a coluna de status do ATS."
                )
            elif n_contratados / n_total > 0.50:
                errors.append(
                    f"⚠️ {n_contratados / n_total:.0%} dos candidatos marcados como Contratado — "
                    "verifique se o arquivo de etapas foi filtrado por status inadvertidamente."
                )

        # ── Consistência temporal ─────────────────────────────────────────────
        if "data_cadastro" in self.df.columns and "data_contratacao" in self.df.columns:
            import pandas as _pd

            def _tz_naive(s: "_pd.Series") -> "_pd.Series":
                s = _pd.to_datetime(s, errors="coerce")
                return s.dt.tz_convert(None) if getattr(s.dt, "tz", None) else s

            dt_cadastro    = _tz_naive(self.df["data_cadastro"])
            dt_contratacao = _tz_naive(self.df["data_contratacao"])
            mask_invalido  = (
                dt_contratacao.notna() &
                dt_cadastro.notna() &
                (dt_contratacao < dt_cadastro)
            )
            n_invalidos = int(mask_invalido.sum())
            if n_invalidos > 0:
                errors.append(
                    f"⚠️ {n_invalidos} registro(s) com data_contratacao anterior a data_cadastro — "
                    "possível erro de exportação do ATS."
                )

        # ── Adesão à EI (apenas se base DigAI foi fornecida) ─────────────────
        if (
            self.total_digai_base > 0
            and self.n_com_digai > 0
            and "data_ei" in self.df.columns
        ):
            com_mask = self.df["processo_seletivo"] == "Com DigAI"
            n_ei = int(self.df.loc[com_mask, "data_ei"].notna().sum())
            adesao = n_ei / self.n_com_digai
            if adesao < 0.30:
                errors.append(
                    f"⚠️ Adesão à EI baixa ({adesao:.0%} dos candidatos Com DigAI têm data_ei). "
                    "Verifique se os arquivos cobrem o mesmo período ou se a coluna data_ei "
                    "está sendo detectada corretamente."
                )

        return errors


@dataclass
class DataCapabilities:
    """
    Descreve quais KPIs estão disponíveis com base nas fontes de dados fornecidas.
    Injetado no relatório como 'capabilities' e consumido pelo dashboard e pelo pipeline.

    Cenários:
      digai_only        — apenas base DigAI (Cenário 3)
      digai_hired       — DigAI + base de contratações (Cenário 2)
      digai_ats_full    — DigAI + Step Funnel completo (Cenário 1)
      digai_ats_partial — DigAI + ATS parcial sem etapas completas (Cenário 4)
    """
    scenario: str = "digai_only"

    # Fontes disponíveis
    has_funnel: bool = False
    has_candidatura: bool = False
    has_stage_cols: bool = False
    has_comparison_group: bool = False  # Tem candidatos Sem DigAI

    # KPIs disponíveis
    can_calc_funil: bool = False
    can_calc_sla: bool = False
    can_calc_assertividade: bool = False
    can_calc_roi: bool = True
    can_calc_saving: bool = True
    can_compare_groups: bool = False
    can_calc_conversion: bool = False
    can_calc_hired: bool = False

    # Motivos de indisponibilidade (chave = nome do KPI, valor = mensagem)
    unavailable: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "scenario":               self.scenario,
            "has_funnel":             self.has_funnel,
            "has_candidatura":        self.has_candidatura,
            "has_stage_cols":         self.has_stage_cols,
            "has_comparison_group":   self.has_comparison_group,
            "can_calc_funil":         self.can_calc_funil,
            "can_calc_sla":           self.can_calc_sla,
            "can_calc_assertividade": self.can_calc_assertividade,
            "can_calc_roi":           self.can_calc_roi,
            "can_calc_saving":        self.can_calc_saving,
            "can_compare_groups":     self.can_compare_groups,
            "can_calc_conversion":    self.can_calc_conversion,
            "can_calc_hired":         self.can_calc_hired,
            "unavailable":            self.unavailable,
        }
