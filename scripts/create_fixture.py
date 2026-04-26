"""
Gera fixture sintética que replica os números do relatório Atento.
Resultado esperado: Savings ~R$1.1M | ROI ~147x | 1.249 contratados | SLA 3.3 dias
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

BASE_DATE = datetime(2025, 2, 20)

def rand_date(base, min_days=0, max_days=5):
    return base + timedelta(days=random.uniform(min_days, max_days))

rows = []

# ── Com DigAI: 49.565 candidatos ─────────────────────────────────────────────
# Contratados: 1.249 | SLA médio: 3.3 dias
n_com = 49_565
n_contratados_com = 1_249
n_reprovados_com  = 11_753
n_desistiu_com    = 2_609
n_em_processo_com = n_com - n_contratados_com - n_reprovados_com - n_desistiu_com

# Funil: Cadastro→EI=29886, EI→Triagem=9559, Triagem→RH=6360, RH→Analise=3174, Analise→Contrat=1436
FUNIL_COM = [49565, 29886, 9559, 6360, 3174, 1436]

for i in range(n_com):
    cadastro = rand_date(BASE_DATE, 0, 17)

    # Determinismo via índice para bater o funil
    has_ei       = i < FUNIL_COM[1]
    has_triagem  = i < FUNIL_COM[2]
    has_rh       = i < FUNIL_COM[3]
    has_analise  = i < FUNIL_COM[4]
    has_contrat  = i < FUNIL_COM[5]

    # Status
    if i < n_contratados_com:
        status = "Contratado"
    elif i < n_contratados_com + n_reprovados_com:
        status = "Reprovado"
    elif i < n_contratados_com + n_reprovados_com + n_desistiu_com:
        status = "Desistiu"
    else:
        status = "Em processo"

    # Datas das etapas (encadeadas)
    ei        = rand_date(cadastro, 1, 4)   if has_ei       else None
    triagem   = rand_date(ei, 0.5, 2)       if has_triagem  else None
    rh        = rand_date(triagem, 0.5, 2)  if has_rh       else None
    analise   = rand_date(rh, 0.3, 1.5)    if has_analise  else None
    contrat   = rand_date(analise, 0.2, 1)  if has_contrat  else None

    # Data final (para SLA)
    if status == "Contratado" and contrat:
        data_final = contrat + timedelta(days=random.uniform(0, 1))
    elif status == "Reprovado":
        last = contrat or analise or rh or triagem or ei or cadastro
        data_final = last + timedelta(days=random.uniform(0, 2))
    else:
        data_final = None

    rows.append({
        "candidato_id":       f"COM-{i+1:06d}",
        "processo_seletivo":  "Com DigAI",
        "status":             status,
        "data_cadastro":      cadastro.date(),
        "data_ei":            ei.date()       if ei       else None,
        "data_triagem":       triagem.date()  if triagem  else None,
        "data_rh":            rh.date()       if rh       else None,
        "data_analise_interna": analise.date() if analise  else None,
        "data_contratacao":   contrat.date()  if contrat  else None,
        "data_final":         data_final.date() if data_final else None,
    })

# ── Sem DigAI: 10.138 candidatos ─────────────────────────────────────────────
# Contratados: 60 | SLA médio: 4.1 dias
n_sem = 10_138
n_contratados_sem = 60
n_reprovados_sem  = 1_858
n_desistiu_sem    = 441
n_em_processo_sem = n_sem - n_contratados_sem - n_reprovados_sem - n_desistiu_sem

FUNIL_SEM = [10138, 7325, 2078, 626, 203, 61]

for i in range(n_sem):
    cadastro = rand_date(BASE_DATE, 0, 17)

    has_ei       = i < FUNIL_SEM[1]
    has_triagem  = i < FUNIL_SEM[2]
    has_rh       = i < FUNIL_SEM[3]
    has_analise  = i < FUNIL_SEM[4]
    has_contrat  = i < FUNIL_SEM[5]

    if i < n_contratados_sem:
        status = "Contratado"
    elif i < n_contratados_sem + n_reprovados_sem:
        status = "Reprovado"
    elif i < n_contratados_sem + n_reprovados_sem + n_desistiu_sem:
        status = "Desistiu"
    else:
        status = "Em processo"

    ei        = rand_date(cadastro, 1, 6)   if has_ei       else None
    triagem   = rand_date(ei, 1, 5)         if has_triagem  else None
    rh        = rand_date(triagem, 1, 4)    if has_rh       else None
    analise   = rand_date(rh, 0.5, 3)      if has_analise  else None
    contrat   = rand_date(analise, 0.1, 0.5) if has_contrat else None

    if status == "Contratado" and contrat:
        data_final = contrat + timedelta(days=random.uniform(0, 1))
    elif status == "Reprovado":
        last = contrat or analise or rh or triagem or ei or cadastro
        data_final = last + timedelta(days=random.uniform(0, 2))
    else:
        data_final = None

    rows.append({
        "candidato_id":       f"SEM-{i+1:06d}",
        "processo_seletivo":  "Sem DigAI",
        "status":             status,
        "data_cadastro":      cadastro.date(),
        "data_ei":            ei.date()       if ei       else None,
        "data_triagem":       triagem.date()  if triagem  else None,
        "data_rh":            rh.date()       if rh       else None,
        "data_analise_interna": analise.date() if analise  else None,
        "data_contratacao":   contrat.date()  if contrat  else None,
        "data_final":         data_final.date() if data_final else None,
    })

df = pd.DataFrame(rows)
out = "/Users/klayvemguimaraes/digai-reports-engine/fixtures/atento_fixture.csv"
df.to_csv(out, index=False)
print(f"✅ Fixture gerada: {out}")
print(f"   Total: {len(df):,} linhas | {df.memory_usage(deep=True).sum()/1024**2:.1f} MB")
print(f"   Com DigAI: {len(df[df.processo_seletivo=='Com DigAI']):,}")
print(f"   Sem DigAI: {len(df[df.processo_seletivo=='Sem DigAI']):,}")
