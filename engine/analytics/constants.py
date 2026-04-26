"""Constantes compartilhadas entre todos os módulos de analytics."""

DEFAULTS = {
    "salario_ta_clt": 4750.0,
    "salario_ta_pj": 2500.0,
    "tempo_entrevista_min": 30,
    "produtividade_pct": 0.60,
    "horas_mes": 176,              # 22 dias × 8h
    "mensalidade_digai": 7600.0,
    "max_entrevistas_ta": 127,     # capacidade máxima contratada do recrutador/mês
                                   # (diferente da capacidade teórica calculada)
}

ETAPAS = [
    ("Cadastro",              "data_cadastro"),
    ("Entrevista Inteligente", "data_ei"),
    ("Triagem",               "data_triagem"),
    ("Entrevista com o RH",   "data_rh"),
    ("Análise Interna",       "data_analise_interna"),
    ("Contratação",           "data_contratacao"),
]

STATUS_CONTRATADO  = "Contratado"
STATUS_DESISTIU    = "Desistiu"
STATUS_REPROVADO   = "Reprovado"
STATUS_EM_PROCESSO = "Em processo"

COM_DIGAI = "Com DigAI"
SEM_DIGAI = "Sem DigAI"
