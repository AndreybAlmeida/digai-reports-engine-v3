"""
DigAI Reports Engine — Analytics Core
Calcula todos os KPIs, ROI, Savings, Funil e Tempos por etapa.
Input: DataFrame com Base de Dados do processo seletivo
Output: dict com todos os dados estruturados para o relatório
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime
import json


# ─── Constantes padrão (sobrescrevíveis por parâmetro) ────────────────────────
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
    ("Cadastro",             "data_cadastro"),
    ("Entrevista Inteligente","data_ei"),
    ("Triagem",              "data_triagem"),
    ("Entrevista com o RH",  "data_rh"),
    ("Análise Interna",      "data_analise_interna"),
    ("Contratação",          "data_contratacao"),
]

STATUS_CONTRATADO = "Contratado"
STATUS_DESISTIU   = "Desistiu"
STATUS_REPROVADO  = "Reprovado"
STATUS_EM_PROCESSO = "Em processo"

COM_DIGAI  = "Com DigAI"
SEM_DIGAI  = "Sem DigAI"


# ─── Carregamento ──────────────────────────────────────────────────────────────

def load_data(path: str) -> pd.DataFrame:
    """
    Carrega CSV ou Excel com otimização de memória para arquivos de 50-90MB.
    Usa dtype explícito para colunas de texto e evita conversão dupla.
    """
    date_col_names = [col for _, col in ETAPAS] + ["data_final"]

    # Dtypes otimizados para reduzir uso de memória
    dtype_map = {
        "processo_seletivo": "category",  # ~8x menos memória que object
        "status":            "category",
    }

    if path.endswith(".xlsx") or path.endswith(".xls"):
        # Excel: leitura direta (openpyxl/xlrd)
        df = pd.read_excel(path, dtype=dtype_map)
    else:
        # CSV grande: leitura otimizada
        # Primeiro detecta separador
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline()
        sep = ";" if first_line.count(";") > first_line.count(",") else ","

        df = pd.read_csv(
            path,
            sep=sep,
            dtype=dtype_map,
            low_memory=False,          # evita dtype misto em colunas grandes
            on_bad_lines="warn",       # não abortar em linhas malformadas
            encoding_errors="replace",
        )

    # Converter colunas de data
    for col in date_col_names:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Normalizar texto
    if "processo_seletivo" in df.columns:
        df["processo_seletivo"] = df["processo_seletivo"].str.strip()
    if "status" in df.columns:
        df["status"] = df["status"].str.strip()

    print(f"   📂 {len(df):,} linhas carregadas | "
          f"Memória: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    return df


# ─── KPIs Principais ──────────────────────────────────────────────────────────

def calcular_kpis(df: pd.DataFrame) -> dict:
    """Calcula KPIs comparativos Com DigAI vs Sem DigAI."""
    result = {}

    for grupo in [COM_DIGAI, SEM_DIGAI]:
        g = df[df["processo_seletivo"] == grupo]
        total = len(g)

        # Contratações: deduplicadas por email para evitar inflar o KPI quando o mesmo
        # candidato aparece em múltiplas vagas do funil (ex: 69k linhas com mesmo email).
        # O grupo "Com DigAI" já foi corretamente segmentado pelo join com a base DigAI.
        _hired_mask = g["status"] == STATUS_CONTRATADO
        _hired_g = g[_hired_mask]
        if "email" in _hired_g.columns:
            _unique_emails = _hired_g["email"].replace("", pd.NA).dropna()
            contratados = int(_unique_emails.nunique()) if len(_unique_emails) > 0 else len(_hired_g)
        else:
            contratados = len(_hired_g)
        contratados_df = _hired_g.copy()

        reprovados  = len(g[g["status"] == STATUS_REPROVADO])
        desistiram  = len(g[g["status"] == STATUS_DESISTIU])
        em_processo = len(g[g["status"] == STATUS_EM_PROCESSO])

        # ── Adesão: % do total Gupy que passou pela EI DigAI ──────────────────────
        # Spec LOGICA_CRUZAMENTO PASSO 5:
        #   ADESAO = N_COM / N_TOTAL   (denominador = TODOS os candidatos do Gupy)
        # "N_COM" = total do grupo Com DigAI; "N_TOTAL" = total geral do df unificado
        n_total_gupy = len(df)
        if grupo == COM_DIGAI:
            adesao = total / n_total_gupy if n_total_gupy > 0 else 0
            na_ei  = total   # todos do grupo Com DigAI fizeram (ou foram triados pela) EI
        else:
            adesao = 0
            na_ei  = 0

        # ── Assertividade: EI necessárias por contratação (eficiência da triagem) ───
        # Spec LOGICA_CRUZAMENTO PASSO 5:
        #   ASSERTIV = N_EI / N_CONT_COM  → quantas EI foram necessárias por contratação
        # Quanto menor, mais precisa a IA. Ex: 30 = 30 entrevistas por contratado.
        # Sem DigAI → None (grupo de controle não tem EI)
        if grupo == COM_DIGAI and contratados > 0:
            assertividade = round(na_ei / contratados, 1)
        else:
            assertividade = None

        # SLA: média dias inscrição → data_final (só contratados)
        # contratados_df já definido acima (Com DigAI: somente quem fez EI; Sem DigAI: todos contratados)
        if "data_cadastro" in g.columns and "data_final" in g.columns:
            def _tz_naive(s):
                s = pd.to_datetime(s, errors="coerce")
                return s.dt.tz_convert(None) if s.dt.tz is not None else s
            contratados_df["sla"] = (
                _tz_naive(contratados_df["data_final"]) - _tz_naive(contratados_df["data_cadastro"])
            ).dt.days
            sla_media   = contratados_df["sla"].mean()
            sla_mediana = contratados_df["sla"].median()
            sla_min     = contratados_df["sla"].min()
            sla_max     = contratados_df["sla"].max()
        else:
            sla_media = sla_mediana = sla_min = sla_max = None

        taxa_contratacao = contratados / total if total > 0 else 0

        # Vagas distintas (só para Com DigAI)
        vagas = 0
        if grupo == COM_DIGAI:
            for c in ("vaga", "vaga_cand", "Nome da vaga", "vaga_digai"):
                if c in g.columns:
                    vagas = int(g[c].dropna().nunique())
                    break

        # Score médio e distribuição (só para Com DigAI)
        score_medio_contratados = None
        score_medio_todos       = None
        score_distribuicao      = None
        if grupo == COM_DIGAI:
            for score_col in ("score_ia", "score_inicial_ia", "score"):
                if score_col in contratados_df.columns:
                    scores = pd.to_numeric(contratados_df[score_col], errors="coerce").dropna()
                    if len(scores) > 0:
                        score_medio_contratados = round(float(scores.mean()), 1)
                    break
            for score_col in ("score_ia", "score_inicial_ia", "score"):
                if score_col in g.columns:
                    scores_todos = pd.to_numeric(g[score_col], errors="coerce").dropna()
                    if len(scores_todos) > 0:
                        score_medio_todos = round(float(scores_todos.mean()), 1)
                        bins   = list(range(0, 101, 10))
                        labels = [f"{b}-{b+10}" for b in bins[:-1]]
                        faixas = pd.cut(scores_todos, bins=bins, labels=labels,
                                        right=False, include_lowest=True)
                        dist = faixas.value_counts().sort_index()
                        score_distribuicao = [
                            {"faixa": str(lbl), "n": int(cnt)}
                            for lbl, cnt in dist.items()
                        ]
                    break

        result[grupo] = {
            "total": total,
            "contratados": contratados,
            "reprovados": reprovados,
            "desistiram": desistiram,
            "em_processo": em_processo,
            "adesao": adesao,
            "assertividade": assertividade,
            "sla_media": round(sla_media, 1) if sla_media and not np.isnan(sla_media) else None,
            "sla_mediana": round(sla_mediana, 1) if sla_mediana and not np.isnan(sla_mediana) else None,
            "sla_min": sla_min,
            "sla_max": sla_max,
            "taxa_contratacao": taxa_contratacao,
            "vagas": vagas,
            "score_medio_contratados": score_medio_contratados,
            "score_medio_todos":       score_medio_todos,
            "score_distribuicao":      score_distribuicao,
        }

    # Delta entre grupos
    com = result[COM_DIGAI]
    sem = result[SEM_DIGAI]
    result["delta"] = {
        # adesão agora é % do total → diferença em pp
        "adesao":           round((com["adesao"] - sem["adesao"]) * 100, 2),
        # assertividade é ratio (EI/contratado) → sem delta cross-group (grupos diferentes)
        "assertividade":    com.get("assertividade"),
        "sla":              round((com["sla_media"] or 0) - (sem["sla_media"] or 0), 1),
        "contratacoes":     com["contratados"] - sem["contratados"],
        "taxa_contratacao": round((com["taxa_contratacao"] - sem["taxa_contratacao"]) * 100, 2),
    }

    return result


# ─── Calculadora ROI ───────────────────────────────────────────────────────────

def _infer_n_meses(df: pd.DataFrame, params: dict) -> int:
    """
    Infere número de meses do período analisado.
    Spec: ROI = saving / (mensalidade × n_meses) — nunca dividir pelo valor de 1 mês
    quando o período é maior.
    """
    # 1. Explícito nos params
    if "n_meses" in params and params["n_meses"] > 0:
        return int(params["n_meses"])

    # 2. Parseia string "MM/YYYY a MM/YYYY" do campo período
    periodo = params.get("periodo", "")
    m = re.findall(r"(\d{1,2})[/\-](\d{4})", periodo)
    if len(m) >= 2:
        try:
            from dateutil.relativedelta import relativedelta as rd
            import datetime
            d_ini = datetime.date(int(m[0][1]), int(m[0][0]), 1)
            d_fim = datetime.date(int(m[-1][1]), int(m[-1][0]), 1)
            delta  = (d_fim.year - d_ini.year) * 12 + (d_fim.month - d_ini.month) + 1
            if delta > 0:
                return delta
        except Exception:
            pass

    # 3. Infere pelo range de data_cadastro no dataframe
    if "data_cadastro" in df.columns:
        dates = pd.to_datetime(df["data_cadastro"], errors="coerce").dropna()
        if len(dates) > 0:
            delta = ((dates.max().year - dates.min().year) * 12
                     + (dates.max().month - dates.min().month) + 1)
            return max(1, min(int(delta), 36))

    return 1


def calcular_roi(df: pd.DataFrame, params: dict) -> dict:
    p = {**DEFAULTS, **params}

    # Número de meses do período (crítico: não dividir mensalidade por 1 mês se período > 1)
    n_meses = _infer_n_meses(df, params)

    # Fonte primária: total da base DigAI (todas as entrevistas realizadas)
    total_entrevistas_ia = df.attrs.get("total_digai_base", 0)
    # Fallback 1: conta data_ei preenchida no dataframe unificado
    if total_entrevistas_ia == 0 and "data_ei" in df.columns:
        total_entrevistas_ia = int(df[
            (df["processo_seletivo"] == COM_DIGAI) & df["data_ei"].notna()
        ].shape[0])
    # Fallback 2: total Com DigAI (todos passaram pela EI por definição)
    if total_entrevistas_ia == 0:
        total_entrevistas_ia = int((df["processo_seletivo"] == COM_DIGAI).sum())

    horas_trabalhadas   = p["horas_mes"]
    horas_em_entrevistas = horas_trabalhadas * p["produtividade_pct"]
    minutos_em_entrevistas = horas_em_entrevistas * 60
    qtd_entrevistas_ta  = minutos_em_entrevistas / p["tempo_entrevista_min"]

    # Custo por entrevista TA: usa a capacidade máxima contratada (max_entrevistas_ta)
    # Isso replica o modelo original: R$4.750 / 127 = R$37,40 → Savings = 29.886 × R$37,40 ≈ R$1,1M
    max_ta = p.get("max_entrevistas_ta", qtd_entrevistas_ta)
    custo_por_entrevista_ta = p["salario_ta_clt"] / max_ta if max_ta > 0 else 0
    custo_por_entrevista_ia = p["mensalidade_digai"] / total_entrevistas_ia if total_entrevistas_ia > 0 else 0
    economia_por_entrevista = custo_por_entrevista_ta - custo_por_entrevista_ia

    savings = total_entrevistas_ia * economia_por_entrevista
    # Spec: roi = saving / (mensalidade × n_meses) — investimento total do período
    investimento_total = p["mensalidade_digai"] * n_meses
    roi = savings / investimento_total if investimento_total > 0 else 0

    # Alerta SLA negativo (spec PASSO 4)
    if "data_cadastro" in df.columns and "data_final" in df.columns:
        def _tz_naive(s):
            s = pd.to_datetime(s, errors="coerce")
            return s.dt.tz_convert(None) if s.dt.tz is not None else s
        sla_check = (_tz_naive(df["data_final"]) - _tz_naive(df["data_cadastro"])).dt.days
        n_neg = (sla_check < 0).sum()
        if n_neg > 0:
            print(f"   ⚠️  {n_neg} candidatos com SLA negativo — possível erro de data no ATS")

    return {
        "horas_trabalhadas": horas_trabalhadas,
        "horas_em_entrevistas": round(horas_em_entrevistas, 1),
        "minutos_em_entrevistas": round(minutos_em_entrevistas, 0),
        "qtd_entrevistas_ta": round(qtd_entrevistas_ta, 0),
        "total_entrevistas_ia": total_entrevistas_ia,
        "custo_por_entrevista_ta": round(custo_por_entrevista_ta, 2),
        "custo_por_entrevista_ia": round(custo_por_entrevista_ia, 2),
        "economia_por_entrevista": round(economia_por_entrevista, 2),
        "savings": round(savings, 2),
        "roi": round(roi, 2),
        "n_meses": n_meses,
        "investimento_total": round(investimento_total, 2),
        "mensalidade_digai": p["mensalidade_digai"],
        "salario_ta_clt": p["salario_ta_clt"],
        "tempo_entrevista_min": p["tempo_entrevista_min"],
        "produtividade_pct": p["produtividade_pct"],
    }


# ─── Origem Geográfica dos Candidatos ────────────────────────────────────────

_DDD_ESTADO = {
    '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP',
    '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
    '21': 'RJ', '22': 'RJ', '24': 'RJ',
    '27': 'ES', '28': 'ES',
    '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG',
    '37': 'MG', '38': 'MG',
    '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
    '47': 'SC', '48': 'SC', '49': 'SC',
    '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
    '61': 'DF', '62': 'GO', '63': 'TO', '64': 'GO',
    '65': 'MT', '66': 'MT', '67': 'MS', '68': 'AC', '69': 'RO',
    '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
    '79': 'SE',
    '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE',
    '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
    '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA',
    '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA',
}

_ESTADO_NOME = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Dist. Fed.', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte', 'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins',
}


def _ddd_to_estado(phone_str: str) -> str:
    import re as _re
    digits = _re.sub(r'\D', '', str(phone_str))
    if len(digits) >= 10:
        ddd = digits[:2] if digits[0] != '0' else digits[1:3]
        return _DDD_ESTADO.get(ddd[:2], 'Outro')
    return 'Sem DDD'


def calcular_origem_candidatos(df: pd.DataFrame) -> list:
    """
    Distribuição geográfica por estado — candidatos Com DigAI.

    Fontes (por prioridade):
    1. Coluna 'estado_digai' (vinda diretamente da base DigAI — mais precisa)
    2. DDD do telefone (fallback quando estado não disponível)
    """
    mask_com = df["processo_seletivo"] == COM_DIGAI
    tmp = df[mask_com].copy()
    if tmp.empty:
        return []

    # Fonte 1: estado direto da base DigAI
    if "estado_digai" in tmp.columns and tmp["estado_digai"].notna().any():
        tmp["_estado_raw"] = tmp["estado_digai"].fillna("").astype(str).str.strip()
        # Normaliza: alguns exports têm UF (2 letras), outros têm nome completo
        def _normaliza_estado(v: str) -> str:
            v = v.strip()
            if len(v) == 2:
                return v.upper()
            # Tenta lookup reverso nome → UF
            for uf, nome in _ESTADO_NOME.items():
                if nome.lower() == v.lower():
                    return uf
            return v if v else "Sem Estado"

        tmp["_estado"] = tmp["_estado_raw"].apply(_normaliza_estado)
        tmp = tmp[tmp["_estado"].ne("")]
        source = "estado_digai"

    # Fonte 2: DDD do telefone
    else:
        phone_col = next((c for c in ('phone', 'phone_raw') if c in tmp.columns), None)
        if phone_col is None or not tmp[phone_col].astype(str).ne('').any():
            return []
        tmp["_estado"] = tmp[phone_col].astype(str).apply(_ddd_to_estado)
        source = "ddd"

    count_col = 'email' if 'email' in tmp.columns else "_estado"
    grp = (tmp.groupby('_estado')
           .agg(total=(count_col, 'count'))
           .reset_index()
           .sort_values('total', ascending=False))

    # Remove "Sem DDD" / "Sem Estado" / "Outro" apenas se houver dados reais
    grp_validos = grp[~grp['_estado'].isin(['Sem DDD', 'Sem Estado', 'Outro', ''])]
    if not grp_validos.empty:
        grp = pd.concat([grp_validos, grp[grp['_estado'].isin(['Sem DDD', 'Sem Estado', 'Outro'])]])

    total_geral = int(grp['total'].sum())
    result = [
        {
            'uf':    str(row['_estado']),
            'nome':  _ESTADO_NOME.get(str(row['_estado']), str(row['_estado'])),
            'total': int(row['total']),
            'pct':   round(int(row['total']) / total_geral * 100, 1) if total_geral else 0,
            'source': source,
        }
        for _, row in grp.iterrows()
    ]
    return result


# ─── Funil de Conversão ────────────────────────────────────────────────────────

def calcular_funil(df: pd.DataFrame) -> list[dict]:
    """Retorna lista de etapas com contagem e % para Com e Sem DigAI."""
    funil = []
    totais = {
        COM_DIGAI: len(df[df["processo_seletivo"] == COM_DIGAI]),
        SEM_DIGAI: len(df[df["processo_seletivo"] == SEM_DIGAI]),
    }

    prev_com = totais[COM_DIGAI]
    prev_sem = totais[SEM_DIGAI]

    for nome_etapa, col in ETAPAS:
        if col not in df.columns:
            continue

        count_com = len(df[(df["processo_seletivo"] == COM_DIGAI) & (df[col].notna())])
        count_sem = len(df[(df["processo_seletivo"] == SEM_DIGAI) & (df[col].notna())])

        pct_com = count_com / totais[COM_DIGAI] if totais[COM_DIGAI] > 0 else 0
        pct_sem = count_sem / totais[SEM_DIGAI] if totais[SEM_DIGAI] > 0 else 0

        # Drop-off em relação à etapa anterior
        dropoff_com = (prev_com - count_com) / prev_com if prev_com > 0 else 0
        dropoff_sem = (prev_sem - count_sem) / prev_sem if prev_sem > 0 else 0

        funil.append({
            "etapa":       nome_etapa,
            "com_digai":   count_com,
            "sem_digai":   count_sem,
            "pct_com":     round(pct_com * 100, 1),
            "pct_sem":     round(pct_sem * 100, 1),
            "dropoff_com": round(-dropoff_com * 100, 1),
            "dropoff_sem": round(-dropoff_sem * 100, 1),
        })

        prev_com = count_com
        prev_sem = count_sem

    # Adicionar linha de Contratados ao final
    # Com DigAI: apenas candidatos que fizeram a EI (deduplicados por email), consistente com calcular_kpis()
    col_final = "status"
    if "_in_digai" in df.columns:
        _f_mask = (df["processo_seletivo"] == COM_DIGAI) & (df[col_final] == STATUS_CONTRATADO) & df["_in_digai"].fillna(False).astype(bool)
        _f_df = df[_f_mask]
        if "email" in df.columns:
            _uniq = _f_df["email"].replace("", pd.NA).dropna()
            count_com_final = int(_uniq.nunique()) if len(_uniq) > 0 else len(_f_df)
        else:
            count_com_final = len(_f_df)
    else:
        count_com_final = len(df[(df["processo_seletivo"] == COM_DIGAI) & (df[col_final] == STATUS_CONTRATADO)])
    count_sem_final = len(df[(df["processo_seletivo"] == SEM_DIGAI) & (df[col_final] == STATUS_CONTRATADO)])
    funil.append({
        "etapa":       "Contratados",
        "com_digai":   count_com_final,
        "sem_digai":   count_sem_final,
        "pct_com":     round(count_com_final / totais[COM_DIGAI] * 100, 2) if totais[COM_DIGAI] > 0 else 0,
        "pct_sem":     round(count_sem_final / totais[SEM_DIGAI] * 100, 2) if totais[SEM_DIGAI] > 0 else 0,
        "dropoff_com": None,
        "dropoff_sem": None,
    })

    return funil


# ─── Tempo Médio por Etapa ────────────────────────────────────────────────────

def calcular_tempo_por_etapa(df: pd.DataFrame) -> list[dict]:
    """Calcula tempo médio (dias) em cada etapa para Com e Sem DigAI."""
    tempos = []
    etapas_com_next = list(zip(ETAPAS, ETAPAS[1:]))

    for (nome, col_atual), (_, col_prox) in etapas_com_next:
        if col_atual not in df.columns or col_prox not in df.columns:
            continue

        for grupo in [COM_DIGAI, SEM_DIGAI]:
            g = df[(df["processo_seletivo"] == grupo) & df[col_atual].notna() & df[col_prox].notna()].copy()
            def _naive(s):
                s = pd.to_datetime(s, errors="coerce")
                if s.dt.tz is not None:
                    s = s.dt.tz_convert(None)
                return s
            g["tempo"] = (_naive(g[col_prox]) - _naive(g[col_atual])).dt.days
            g = g[g["tempo"] >= 0]

            if grupo == COM_DIGAI:
                tempo_com = round(g["tempo"].mean(), 1) if len(g) > 0 else None
            else:
                tempo_sem = round(g["tempo"].mean(), 1) if len(g) > 0 else None

        if tempo_com is not None and tempo_sem is not None:
            diferenca = round(tempo_com - tempo_sem, 1)
            impacto = "✅ DigAI mais rápida" if diferenca < 0 else "⚠️ DigAI mais lenta"
        else:
            diferenca = None
            impacto = "N/A"

        tempos.append({
            "etapa":     nome,
            "com_digai": tempo_com,
            "sem_digai": tempo_sem,
            "diferenca": diferenca,
            "impacto":   impacto,
        })

    return tempos


# ─── Status dos Candidatos (Pivot A) ─────────────────────────────────────────

def calcular_status(df: pd.DataFrame) -> list[dict]:
    totais = {
        COM_DIGAI: len(df[df["processo_seletivo"] == COM_DIGAI]),
        SEM_DIGAI: len(df[df["processo_seletivo"] == SEM_DIGAI]),
    }
    statuses = [STATUS_CONTRATADO, STATUS_REPROVADO, STATUS_DESISTIU, STATUS_EM_PROCESSO]
    resultado = []
    for s in statuses:
        c = len(df[(df["processo_seletivo"] == COM_DIGAI) & (df["status"] == s)])
        s_ = len(df[(df["processo_seletivo"] == SEM_DIGAI) & (df["status"] == s)])
        pct_c = c / totais[COM_DIGAI] if totais[COM_DIGAI] > 0 else 0
        pct_s = s_ / totais[SEM_DIGAI] if totais[SEM_DIGAI] > 0 else 0
        resultado.append({
            "status":    s,
            "com_digai": c,
            "pct_com":   round(pct_c * 100, 1),
            "sem_digai": s_,
            "pct_sem":   round(pct_s * 100, 1),
            "delta_pp":  round((pct_c - pct_s) * 100, 1),
        })
    resultado.append({
        "status":    "TOTAL",
        "com_digai": totais[COM_DIGAI],
        "pct_com":   100.0,
        "sem_digai": totais[SEM_DIGAI],
        "pct_sem":   100.0,
        "delta_pp":  None,
    })
    return resultado


def calcular_assertividade_ia(df: pd.DataFrame) -> dict:
    """
    Concordância entre o score da IA e a decisão do recrutador.
    Proxy de assertividade para Tier 3 (sem dados de contratação).

    score_editado=True → recrutador sobrescreveu o score.
    Concordância = % que NÃO foram editados.
    """
    com = df[df["processo_seletivo"] == COM_DIGAI] if "processo_seletivo" in df.columns else df
    total = len(com)

    if total == 0 or "score_editado" not in com.columns:
        return {"concordancia": None, "total": total, "editados": 0, "sem_dados": True}

    editados = int(com["score_editado"].fillna(False).astype(bool).sum())
    concordancia = round((1 - editados / total) * 100, 1) if total > 0 else None

    return {
        "concordancia": concordancia,
        "total": total,
        "editados": editados,
        "sem_dados": concordancia is None,
    }


# ─── Diagnóstico de Qualidade (LOGICA_CRUZAMENTO PASSO 7) ─────────────────────

def diagnostico_qualidade(df: pd.DataFrame) -> list[str]:
    """
    Retorna lista de alertas de qualidade sobre os dados cruzados.
    Nunca lança exceção — apenas acumula alertas de texto.
    """
    alertas = []
    n_total = len(df)
    if n_total == 0:
        return ["⚠️ DataFrame vazio — nenhum dado processado."]

    # 1. Volume de cruzamento
    if "processo_seletivo" in df.columns:
        n_com = (df["processo_seletivo"] == COM_DIGAI).sum()
        pct   = n_com / n_total * 100
        if pct < 5:
            alertas.append(
                f"⚠️ Apenas {pct:.1f}% dos candidatos do Gupy cruzaram com a DigAI. "
                "Verificar se os arquivos são do mesmo período e mesmo cliente."
            )

    # 2. SLA negativo
    for col in ("_sla_dias", "sla_dias", "sla"):
        if col in df.columns:
            n_neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
            if n_neg > 0:
                alertas.append(
                    f"⚠️ {n_neg:,} candidatos com SLA negativo — excluídos do cálculo. "
                    "Verificar datas no ATS."
                )
            break

    # 3. Contratados sem data de contratação
    if "status" in df.columns and "data_contratacao" in df.columns:
        sem_data = (
            (df["status"] == STATUS_CONTRATADO) & df["data_contratacao"].isna()
        ).sum()
        if sem_data > 0:
            alertas.append(
                f"⚠️ {sem_data:,} contratados sem data de contratação — SLA parcial."
            )

    # 4. Grupos zerados
    if "processo_seletivo" in df.columns:
        n_com = (df["processo_seletivo"] == COM_DIGAI).sum()
        n_sem = (df["processo_seletivo"] == SEM_DIGAI).sum()
        if n_com == 0:
            alertas.append(
                "❌ CRÍTICO: nenhum candidato identificado como Com DigAI. "
                "Verificar cruzamento de email ou nome da etapa no ATS."
            )
        if n_sem == 0:
            alertas.append(
                "⚠️ Todos os candidatos estão no grupo Com DigAI — sem grupo de controle."
            )

    # 5. Duplicatas exatas (mesmo email + mesma vaga)
    if "email" in df.columns and "vaga" in df.columns:
        dup_mask = df["email"].ne("") & df["vaga"].ne("")
        n_dup = df[dup_mask].duplicated(subset=["email", "vaga"]).sum()
        if n_dup > 0:
            alertas.append(
                f"⚠️ {n_dup:,} linhas com mesmo email + mesma vaga (duplicatas exatas). "
                "Esses registros foram mantidos — verifique se é reprocessamento ou erro de export."
            )

    # 6. Multi-vaga (normal)
    if "email" in df.columns:
        emails_ne = df["email"].ne("")
        multi = df[emails_ne].duplicated(subset=["email"], keep=False).sum()
        if multi > 0:
            alertas.append(
                f"ℹ️ {multi:,} linhas com mesmo email em vagas diferentes (candidato em múltiplas vagas — normal)."
            )

    return alertas


# ─── Insights e Veredicto ─────────────────────────────────────────────────────

def gerar_insights(kpis: dict, roi: dict) -> dict:
    """Gera veredicto e pontos de atenção baseados nos números."""
    if not kpis or "_unavailable" in kpis or COM_DIGAI not in kpis:
        return {
            "veredicto": "ℹ️ DADOS PARCIAIS",
            "cor_veredicto": "#94A3B8",
            "pontos_positivos": [],
            "pontos_atencao": ["Relatório gerado apenas com a base DigAI — dados comparativos não disponíveis."],
        }
    com = kpis[COM_DIGAI]
    sem = kpis.get(SEM_DIGAI, {})
    delta = kpis.get("delta", {})

    # Sem grupo de controle (sem Sem DigAI): insights limitados
    if not sem or sem.get("total", 0) == 0:
        pontos = []
        if roi and roi.get("roi", 0) >= 2:
            pontos.append(f"ROI estimado de {roi.get('roi',0):.0f}x — savings de R$ {roi.get('savings',0):,.2f} no período")
        total_ei = com.get("total", 0)
        if total_ei:
            pontos.append(f"{total_ei:,} candidatos entrevistados pela DigAI no período")
        return {
            "veredicto": "✅ BASE DIGAI PROCESSADA",
            "cor_veredicto": "#20BD5A",
            "pontos_positivos": pontos,
            "pontos_atencao": ["Sem grupo de controle — envie o Step Funnel do ATS para comparativos completos."],
        }

    pontos_positivos = []
    pontos_atencao   = []

    # Contratações
    if com["contratados"] > sem["contratados"]:
        mult = com["contratados"] / sem["contratados"] if sem["contratados"] > 0 else 0
        pontos_positivos.append(
            f"Contratações {mult:.1f}x maior com DigAI ({com['contratados']} vs {sem['contratados']})"
        )

    # Assertividade (ratio EI/contratado — menor = mais eficiente)
    _assert_com = com.get("assertividade")
    _assert_sem = sem.get("assertividade")
    if _assert_com is not None and _assert_sem is not None:
        if _assert_com < _assert_sem:
            pontos_positivos.append(
                f"Assertividade melhor com DigAI ({_assert_com:.1f} EI/contratado vs {_assert_sem:.1f})"
            )
        else:
            pontos_atencao.append(
                f"Assertividade inferior com DigAI ({_assert_com:.1f} EI/contratado vs {_assert_sem:.1f})"
            )

    # SLA — só compara quando há grupo de controle com SLA calculado
    _sla_delta = delta.get("sla", 0) or 0
    _sem_sla   = sem.get("sla_media")
    _com_sla   = com.get("sla_media")
    if _com_sla is not None and _sem_sla is not None:
        if _sla_delta < 0:
            pontos_positivos.append(
                f"SLA {abs(_sla_delta):.1f} dias mais rápido com DigAI "
                f"({_com_sla} vs {_sem_sla} dias)"
            )
        elif _sla_delta > 0:
            pontos_atencao.append(
                f"SLA {_sla_delta:+.1f} dias mais lento com DigAI"
            )

    # Adesão
    _adesao_delta = delta.get("adesao", 0) or 0
    if _adesao_delta < 0:
        pontos_atencao.append(
            f"Adesão menor com DigAI ({_adesao_delta:+.1f} pp) — mais candidatos desistem na EI"
        )
    elif _adesao_delta > 0:
        pontos_positivos.append(
            f"Adesão {_adesao_delta:+.1f} pp maior com DigAI"
        )

    # ROI
    if roi["roi"] >= 10:
        pontos_positivos.append(
            f"ROI de {roi['roi']:.0f}x — savings de R$ {roi['savings']:,.2f} no período"
        )

    # Veredicto
    score = len(pontos_positivos) - len(pontos_atencao)
    if score >= 2:
        veredicto = "✅ PERFORMANDO BEM"
        cor_veredicto = "#20BD5A"
    elif score >= 0:
        veredicto = "⚠️ ATENÇÃO NECESSÁRIA"
        cor_veredicto = "#f59e0b"
    else:
        veredicto = "❌ PERFORMANCE ABAIXO DO ESPERADO"
        cor_veredicto = "#dc2626"

    return {
        "veredicto":       veredicto,
        "cor_veredicto":   cor_veredicto,
        "pontos_positivos": pontos_positivos,
        "pontos_atencao":  pontos_atencao,
    }


# ─── Funil Dinâmico (etapas reais do Gupy) ────────────────────────────────────

def calcular_funil_dinamico(df: pd.DataFrame) -> list[dict]:
    """
    Funil usando as colunas stage_N_entry detectadas automaticamente.
    Retorna lista de etapas com contagem e % para Com e Sem DigAI.
    """
    stage_cols = df.attrs.get("stage_cols", {})
    if not stage_cols:
        print("   Nenhuma etapa de processo detectada — SLA por etapa nao disponivel")
        return calcular_funil(df)  # fallback

    funil = []
    totais = {
        COM_DIGAI: len(df[df["processo_seletivo"] == COM_DIGAI]),
        SEM_DIGAI: len(df[df["processo_seletivo"] == SEM_DIGAI]),
    }

    prev = {COM_DIGAI: totais[COM_DIGAI], SEM_DIGAI: totais[SEM_DIGAI]}

    # Cadastro (total de inscritos)
    funil.append({
        "etapa":       "Cadastro",
        "com_digai":   totais[COM_DIGAI],
        "sem_digai":   totais[SEM_DIGAI],
        "pct_com":     100.0,
        "pct_sem":     100.0,
        "dropoff_com": 0.0,
        "dropoff_sem": 0.0,
    })

    # Entrevista DigAI (data_ei) — antes das etapas Gupy
    if "data_ei" in df.columns:
        c_com = len(df[(df["processo_seletivo"] == COM_DIGAI) & df["data_ei"].notna()])
        c_sem = len(df[(df["processo_seletivo"] == SEM_DIGAI) & df["data_ei"].notna()])
        if c_com > 0 or c_sem > 0:
            funil.append({
                "etapa":       "Entrevista DigAI",
                "com_digai":   c_com,
                "sem_digai":   c_sem,
                "pct_com":     round(c_com / totais[COM_DIGAI] * 100, 1) if totais[COM_DIGAI] > 0 else 0,
                "pct_sem":     round(c_sem / totais[SEM_DIGAI] * 100, 1) if totais[SEM_DIGAI] > 0 else 0,
                "dropoff_com": round((prev[COM_DIGAI] - c_com) / prev[COM_DIGAI] * 100, 1) if prev[COM_DIGAI] > 0 else 0,
                "dropoff_sem": round((prev[SEM_DIGAI] - c_sem) / prev[SEM_DIGAI] * 100, 1) if prev[SEM_DIGAI] > 0 else 0,
            })
            prev = {COM_DIGAI: c_com, SEM_DIGAI: c_sem}

    # Etapas Gupy (dinâmicas)
    added_names = set()
    for n in sorted(stage_cols.keys()):
        name_col  = stage_cols[n].get("name")
        entry_col = stage_cols[n].get("entry")
        if not entry_col or entry_col not in df.columns:
            continue

        # Pega o nome mais comum da etapa
        if name_col and name_col in df.columns:
            top_names = df[name_col].value_counts()
            etapa_nome = top_names.index[0] if len(top_names) > 0 else f"Etapa {n}"
        else:
            etapa_nome = f"Etapa {n}"

        if etapa_nome in added_names:
            continue
        added_names.add(etapa_nome)

        c_com = len(df[(df["processo_seletivo"] == COM_DIGAI) & df[entry_col].notna()])
        c_sem = len(df[(df["processo_seletivo"] == SEM_DIGAI) & df[entry_col].notna()])

        funil.append({
            "etapa":       etapa_nome,
            "com_digai":   c_com,
            "sem_digai":   c_sem,
            "pct_com":     round(c_com / totais[COM_DIGAI] * 100, 1) if totais[COM_DIGAI] > 0 else 0,
            "pct_sem":     round(c_sem / totais[SEM_DIGAI] * 100, 1) if totais[SEM_DIGAI] > 0 else 0,
            "dropoff_com": round((prev[COM_DIGAI] - c_com) / prev[COM_DIGAI] * 100, 1) if prev[COM_DIGAI] > 0 else 0,
            "dropoff_sem": round((prev[SEM_DIGAI] - c_sem) / prev[SEM_DIGAI] * 100, 1) if prev[SEM_DIGAI] > 0 else 0,
        })
        prev = {COM_DIGAI: c_com, SEM_DIGAI: c_sem}

    return funil


def calcular_tempo_dinamico(df: pd.DataFrame) -> list[dict]:
    """
    Tempo médio em cada etapa usando stage_N_days detectados dinamicamente.
    """
    stage_cols = df.attrs.get("stage_cols", {})
    if not stage_cols:
        print("   Nenhuma etapa de processo detectada — SLA por etapa nao disponivel")
        return calcular_tempo_por_etapa(df)

    tempos = []
    for n in sorted(stage_cols.keys()):
        name_col = stage_cols[n].get("name")
        days_col = stage_cols[n].get("days")
        if not days_col or days_col not in df.columns:
            continue

        if name_col and name_col in df.columns:
            etapa_nome = df[name_col].value_counts().index[0] if df[name_col].notna().any() else f"Etapa {n}"
        else:
            etapa_nome = f"Etapa {n}"

        for grupo in [COM_DIGAI, SEM_DIGAI]:
            sub = df[(df["processo_seletivo"] == grupo) & df[days_col].notna()]
            vals = pd.to_numeric(sub[days_col], errors="coerce").dropna()
            vals = vals[vals >= 0]
            if grupo == COM_DIGAI:
                tempo_com = round(vals.mean(), 1) if len(vals) > 0 else None
            else:
                tempo_sem = round(vals.mean(), 1) if len(vals) > 0 else None

        if tempo_com is not None or tempo_sem is not None:
            dif = round((tempo_com or 0) - (tempo_sem or 0), 1)
            tempos.append({
                "etapa":     etapa_nome,
                "com_digai": tempo_com,
                "sem_digai": tempo_sem,
                "diferenca": dif if tempo_com is not None and tempo_sem is not None else None,
                "impacto":   ("✅ DigAI mais rápida" if dif < 0 else "⚠️ DigAI mais lenta")
                             if tempo_com is not None and tempo_sem is not None else "N/A",
            })

    return tempos


# ─── Runner principal ──────────────────────────────────────────────────────────

def gerar_relatorio(path: str, params: dict = None) -> dict:
    """
    Ponto de entrada principal do engine.
    Retorna dict completo com todos os dados do relatório.
    """
    params = params or {}
    df = load_data(path)

    kpis   = calcular_kpis(df)
    roi    = calcular_roi(df, params)
    funil  = calcular_funil(df)
    tempos = calcular_tempo_por_etapa(df)
    status = calcular_status(df)
    insights = gerar_insights(kpis, roi)

    # Funil e tempos dinâmicos (se o df tiver stage_cols do Gupy)
    funil_din  = calcular_funil_dinamico(df)
    tempos_din = calcular_tempo_dinamico(df)

    return {
        "meta": {
            "cliente":     params.get("cliente_nome", "Cliente"),
            "periodo":     params.get("periodo", ""),
            "gerado_em":   datetime.now().strftime("%d/%m/%Y %H:%M"),
            "logo_url":    params.get("logo_url", ""),
        },
        "kpis":         kpis,
        "roi":          roi,
        "funil":        funil,          # funil legado (6 etapas fixas)
        "funil_din":    funil_din,      # funil dinâmico com etapas reais
        "tempos":       tempos,
        "tempos_din":   tempos_din,
        "status":       status,
        "insights":     insights,
    }


def gerar_relatorio_from_sources(
    funnel_path: str,
    candidatura_path,
    digai_path,
    params: dict = None,
) -> dict:
    """
    Entry point para o novo pipeline com 3 fontes.
    Carrega, une, segmenta e calcula relatório completo.
    """
    from .ingestion import load_gupy_funnel, load_gupy_candidatura, load_digai_base
    from .segmentation import build_unified
    from .schema import SegmentationResult

    import gc
    params = params or {}
    print("\n📥 Carregando fontes...")
    funnel_result     = load_gupy_funnel(funnel_path)
    candidatura_result = load_gupy_candidatura(candidatura_path) if candidatura_path else None
    digai_result       = load_digai_base(digai_path) if digai_path else None

    print("\n🔗 Unificando e segmentando...")
    seg_result = build_unified(funnel_result, candidatura_result, digai_result)

    # Libera os resultados intermediários para economizar memória
    del funnel_result, candidatura_result, digai_result
    gc.collect()

    # Desempacota SegmentationResult — usa df para todo o processamento downstream
    df = seg_result.df
    # Propaga metadados para df.attrs (compatibilidade com código legado em excel_gen, dimensions, etc.)
    df.attrs["stage_cols"]       = seg_result.stage_cols
    df.attrs["ei_stage_col"]     = seg_result.ei_stage_col
    df.attrs["strategy"]         = seg_result.strategy
    df.attrs["n_stages"]         = len(seg_result.stage_cols)
    df.attrs["total_digai_base"] = seg_result.total_digai_base
    del seg_result
    gc.collect()

    print("\n📊 Calculando métricas...")
    params = params or {}
    alertas_qualidade = diagnostico_qualidade(df)
    for a in alertas_qualidade:
        print(f"   {a}")

    # ── Checkpoint: alertas fatais abortam o pipeline imediatamente ───────────
    # Um relatório gerado com 0 candidatos Com DigAI é silenciosamente incorreto.
    # Melhor falhar rápido com mensagem acionável do que entregar dado zerado.
    fatais = [a for a in alertas_qualidade if a.startswith("❌")]
    if fatais:
        raise ValueError(
            "Pipeline abortado — dado crítico detectado:\n" +
            "\n".join(fatais) +
            "\n\nVerifique se os arquivos são do mesmo período e mesmo cliente, "
            "e se os emails do funil coincidem com os da base DigAI."
        )

    kpis   = calcular_kpis(df)
    roi    = calcular_roi(df, params)
    funil  = calcular_funil(df)
    tempos = calcular_tempo_por_etapa(df)
    status = calcular_status(df)
    insights = gerar_insights(kpis, roi)
    funil_din  = calcular_funil_dinamico(df)
    tempos_din = calcular_tempo_dinamico(df)

    meta = {
        "cliente":     params.get("cliente_nome", "Cliente"),
        "periodo":     params.get("periodo", ""),
        "gerado_em":   datetime.now().strftime("%d/%m/%Y %H:%M"),
        "logo_url":    params.get("logo_url", ""),
        "strategy":    df.attrs.get("strategy", ""),
    }

    narrativa            = gerar_narrativa(kpis, roi, meta)
    mapa_vagas           = calcular_mapa_vagas(df)
    periodo_comparativo  = calcular_periodo_comparativo(df)
    origem_candidatos    = calcular_origem_candidatos(df)

    return {
        "meta":               meta,
        "kpis":               kpis,
        "roi":                roi,
        "funil":              funil,
        "funil_din":          funil_din,
        "tempos":             tempos,
        "tempos_din":         tempos_din,
        "status":             status,
        "insights":           insights,
        "alertas_qualidade":  alertas_qualidade,
        "narrativa":          narrativa,
        "mapa_vagas":         mapa_vagas,
        "periodo_comparativo": periodo_comparativo,
        "origem_candidatos":  origem_candidatos,
        "_df":                df,       # passado internamente para excel_gen (não vai para JSON)
    }


# ─── Narrativa & Novos Blocos ─────────────────────────────────────────────────

def gerar_narrativa(kpis: dict, roi: dict, meta: dict) -> dict:
    """
    Gera narrativa executiva sobre o impacto DigAI no período.
    Retorna headline, destaques, oportunidades e historia (parágrafo).
    Baseado 100% nos dados do cliente — sem benchmarks externos.
    """
    if not kpis or "_unavailable" in kpis:
        kpis = {}
    if not roi or "_unavailable" in roi:
        roi = {}
    com   = kpis.get(COM_DIGAI, {})
    sem   = kpis.get(SEM_DIGAI, {})
    delta = kpis.get("delta", {})

    contratados_com = com.get("contratados", 0)
    contratados_sem = sem.get("contratados", 0)
    total_com       = com.get("total", 0)
    total_sem       = sem.get("total", 0)
    total_geral     = total_com + total_sem

    # Taxa de contratação = % do grupo que foi efetivamente contratado
    taxa_com = round(contratados_com / total_com * 100, 1) if total_com > 0 else 0
    taxa_sem = round(contratados_sem / total_sem * 100, 1) if total_sem > 0 else 0

    # Assertividade = EIs necessárias por contratação (ex: 15.3 = 1 contratado a cada 15.3 EIs)
    # Valor baixo = triagem mais precisa
    assertividade_ratio = com.get("assertividade")  # ratio bruto, já calculado como na_ei/contratados

    savings         = roi.get("savings", 0)
    roi_val         = roi.get("roi", 0)
    total_ei        = roi.get("total_entrevistas_ia", 0)
    custo_ia        = roi.get("custo_por_entrevista_ia", 0)
    custo_ta        = roi.get("custo_por_entrevista_ta", 0)
    sla_com         = com.get("sla_media") or 0
    sla_sem         = sem.get("sla_media") or 0
    adesao_pct      = round((com.get("adesao") or 0) * 100, 1)

    cliente = meta.get("cliente", "A empresa")
    periodo = meta.get("periodo", "no período")

    headline = (
        f"{contratados_com:,} contratações via DigAI em {periodo} "
        f"— taxa de {taxa_com:.1f}% vs {taxa_sem:.1f}% sem triagem IA"
    )

    destaques = []
    if roi_val > 0:
        destaques.append(
            f"ROI de {roi_val:.1f}x — economia de R$ {savings:,.0f} no período "
            f"(custo IA: R$ {custo_ia:.2f}/candidato vs R$ {custo_ta:.2f}/candidato tradicional)"
        )
    if contratados_com > 0 and contratados_sem > 0:
        mult = contratados_com / contratados_sem
        destaques.append(
            f"{mult:.1f}x mais contratações com DigAI: {contratados_com:,} (Com IA) vs {contratados_sem:,} (Sem IA) "
            f"de um total de {total_geral:,} candidatos no funil"
        )
    elif contratados_com > 0:
        destaques.append(
            f"{contratados_com:,} contratações realizadas pelo grupo DigAI "
            f"({taxa_com:.1f}% de taxa de conversão)"
        )
    if taxa_com > taxa_sem and taxa_sem > 0:
        destaques.append(
            f"Taxa de contratação {taxa_com - taxa_sem:.1f} pp maior com DigAI "
            f"({taxa_com:.1f}% vs {taxa_sem:.1f}%)"
        )
    if total_ei > 0:
        destaques.append(
            f"{total_ei:,} entrevistas automatizadas realizadas — "
            f"reduzindo carga operacional do time de RH"
        )
    if sla_com > 0 and sla_sem > 0:
        diff_sla = sla_sem - sla_com
        if diff_sla > 0:
            destaques.append(
                f"Processo {diff_sla:.0f} dias mais ágil com triagem IA "
                f"({sla_com:.0f} vs {sla_sem:.0f} dias médios)"
            )
        else:
            destaques.append(
                f"SLA médio: {sla_com:.0f} dias (Com DigAI) | {sla_sem:.0f} dias (Sem DigAI)"
            )
    if assertividade_ratio and assertividade_ratio > 0:
        destaques.append(
            f"Assertividade da triagem: {assertividade_ratio:.1f} entrevistas por contratação "
            f"— cada {assertividade_ratio:.0f} EIs resultam em 1 contratação"
        )

    oportunidades = []
    if adesao_pct < 50:
        oportunidades.append(
            f"Adesão à EI de {adesao_pct:.1f}% — aumentar engajamento dos candidatos "
            "para aproveitar mais o potencial da triagem IA"
        )
    if sla_com > 0 and sla_sem > 0 and sla_com > sla_sem + 5:
        oportunidades.append(
            f"SLA do grupo DigAI ({sla_com:.0f} dias) está acima do grupo controle ({sla_sem:.0f} dias) "
            "— revisar gargalos após triagem IA"
        )
    if taxa_com < taxa_sem and total_sem > 0:
        oportunidades.append(
            "Taxa de conversão abaixo do grupo controle — revisar critérios de aprovação da IA"
        )
    if not oportunidades:
        oportunidades.append(
            "Expandir o uso da triagem IA para mais vagas e processos seletivos"
        )

    historia = (
        f"{cliente} utilizou a triagem inteligente DigAI em {periodo}. "
        f"Foram realizadas {total_ei:,} entrevistas automatizadas, "
        f"resultando em {contratados_com:,} contratações no grupo Com DigAI "
        f"(taxa de conversão: {taxa_com:.1f}%). "
    )
    if contratados_sem > 0:
        historia += (
            f"O grupo Sem DigAI contabilizou {contratados_sem:,} contratações "
            f"com taxa de {taxa_sem:.1f}%, "
        )
    if roi_val > 0:
        historia += (
            f"gerando economia de R$ {savings:,.0f} e ROI de {roi_val:.1f}x sobre o investimento."
        )
    else:
        historia += "evidenciando o impacto da triagem IA no processo de recrutamento."

    return {
        "headline":      headline,
        "destaques":     destaques,
        "oportunidades": oportunidades,
        "historia":      historia,
    }


def calcular_mapa_vagas(df: pd.DataFrame) -> list:
    """
    Retorna lista das top 20 vagas com total, contratados, assertividade, adesão e SLA médio.

    - assertividade: % dos candidatos Com DigAI que foram contratados (taxa de conversão)
    - adesao: % de todos os candidatos da vaga que passaram pela EI (Com DigAI)
    - sla_medio: dias médios de cadastro → contratação dos contratados Com DigAI
    """
    vaga_col = None
    for c in ("vaga", "vaga_cand", "Nome da vaga", "vaga_digai"):
        if c in df.columns:
            vaga_col = c
            break
    if not vaga_col:
        return []

    com = df[df["processo_seletivo"] == COM_DIGAI]
    if com.empty:
        return []

    def _tz_naive(s):
        s = pd.to_datetime(s, errors="coerce")
        return s.dt.tz_convert(None) if s.dt.tz is not None else s

    result = []
    for vaga, com_grupo in com.groupby(vaga_col, observed=True):
        if not vaga or str(vaga).strip() == "" or pd.isna(vaga):
            continue

        total_com   = len(com_grupo)
        contratados = int((com_grupo["status"] == STATUS_CONTRATADO).sum())

        # assertividade: % dos com-digai contratados (conversão da triagem)
        assertividade = round(contratados / total_com * 100, 1) if total_com > 0 else 0.0

        # adesão: % do total da vaga (com + sem) que fez EI
        total_all = int((df[vaga_col] == vaga).sum()) if vaga_col in df.columns else total_com
        adesao = round(total_com / total_all * 100, 1) if total_all > 0 else None

        # SLA médio: dias de data_cadastro → data_contratacao para contratados
        sla_medio = None
        if (
            contratados > 0 and
            "data_cadastro" in com_grupo.columns and
            "data_contratacao" in com_grupo.columns
        ):
            hired = com_grupo[com_grupo["status"] == STATUS_CONTRATADO]
            sla_days = (
                _tz_naive(hired["data_contratacao"]) - _tz_naive(hired["data_cadastro"])
            ).dt.days
            sla_valid = sla_days[(sla_days >= 0) & sla_days.notna()]
            if len(sla_valid) > 0:
                sla_medio = round(float(sla_valid.mean()), 1)

        result.append({
            "vaga":          str(vaga)[:70],
            "total":         total_com,
            "contratados":   contratados,
            "assertividade": assertividade,
            "adesao":        adesao,
            "sla_medio":     sla_medio,
        })

    result.sort(key=lambda x: x["contratados"], reverse=True)
    return result[:10]


def calcular_periodo_comparativo(df: pd.DataFrame) -> list:
    """
    Breakdown mensal de KPIs (retorna vazio se menos de 2 meses).
    """
    if "_periodo_mensal" not in df.columns:
        if "data_cadastro" not in df.columns:
            return []
        df = df.copy()
        df["_periodo_mensal"] = (
            pd.to_datetime(df["data_cadastro"], errors="coerce")
            .dt.to_period("M")
            .astype(str)
        )

    periodos = sorted(p for p in df["_periodo_mensal"].dropna().unique() if p != "NaT")
    if len(periodos) < 2:
        return []

    result = []
    for periodo in periodos:
        df_p = df[df["_periodo_mensal"] == periodo]
        com  = df_p[df_p["processo_seletivo"] == COM_DIGAI]
        sem  = df_p[df_p["processo_seletivo"] == SEM_DIGAI]
        total_com    = len(com)
        total_sem    = len(sem)
        contratados  = int((com["status"] == STATUS_CONTRATADO).sum())
        assertividade = round(contratados / total_com * 100, 1) if total_com > 0 else 0.0
        result.append({
            "periodo":      periodo,
            "total_com":    total_com,
            "total_sem":    total_sem,
            "contratados":  contratados,
            "assertividade": assertividade,
        })

    return result


# ─── Análise interna de duplicatas e desalinhamento ──────────────────────────

def analisar_qualidade(df: pd.DataFrame) -> dict:
    """
    Análise interna de qualidade dos dados — NUNCA vai para o Excel/dashboard.
    Retorna alertas sobre duplicatas e contratados desalinhados entre bases.
    """
    result = {"duplicatas": {}, "desalinhados": [], "alertas": []}

    # ── Duplicatas ────────────────────────────────────────────────────────────
    if "email" in df.columns and "vaga" in df.columns:
        dup_exatas = int(df.duplicated(subset=["email", "vaga"], keep=False).sum())
        result["duplicatas"]["exatas_email_vaga"] = dup_exatas
        if dup_exatas > 0:
            result["alertas"].append(
                f"⚠️ {dup_exatas} linhas com mesmo email + mesma vaga (duplicatas exatas). "
                "Esses registros foram mantidos — verifique se é reprocessamento ou erro de export."
            )

    if "email" in df.columns:
        dup_soft = int(df.duplicated(subset=["email"], keep=False).sum())
        result["duplicatas"]["soft_email"] = dup_soft
        if dup_soft > 0:
            result["alertas"].append(
                f"ℹ️ {dup_soft} linhas com mesmo email em vagas diferentes (candidato em múltiplas vagas — normal)."
            )

    # ── Contratados desalinhados (estão na base DigAI mas classificados como Sem DigAI) ──
    if all(c in df.columns for c in ("email", "status", "processo_seletivo", "_in_digai")):
        mask = (
            (df["status"] == "Contratado") &
            (df["processo_seletivo"] == "Sem DigAI") &
            (df["_in_digai"] == True)
        )
        desalinhados = df[mask][["email", "nome", "vaga", "status", "processo_seletivo"]].copy()
        result["desalinhados"] = desalinhados.astype(object).fillna("").astype(str).to_dict("records")
        n = len(desalinhados)
        if n > 0:
            result["alertas"].append(
                f"⚠️ {n} contratado(s) encontrados na base DigAI mas classificados como 'Sem DigAI'. "
                "Causa provável: email diferente entre o arquivo de funil e a base DigAI. "
                "Verifique os exemplos em 'desalinhados' para corrigir."
            )

    return result


# ─── CLI para teste rápido ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python analytics.py <caminho_do_arquivo> [cliente_nome] [mensalidade]")
        sys.exit(1)

    path = sys.argv[1]
    params = {
        "cliente_nome":    sys.argv[2] if len(sys.argv) > 2 else "Atento",
        "mensalidade_digai": float(sys.argv[3]) if len(sys.argv) > 3 else 7600.0,
        "periodo":         sys.argv[4] if len(sys.argv) > 4 else "",
    }

    relatorio = gerar_relatorio(path, params)
    print(json.dumps(relatorio, ensure_ascii=False, indent=2, default=str))
