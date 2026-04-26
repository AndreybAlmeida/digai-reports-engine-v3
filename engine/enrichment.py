"""
DigAI Reports Engine — Enriquecimento demográfico

Responsabilidade: adicionar colunas de perfil ao DataFrame unificado
e calcular a distribuição demográfica dos candidatos aprovados.

Funções públicas:
    enrich_dataframe(df)        → adiciona _genero ao df (inplace, retorna df)
    calcular_perfil_aprovados(df) → dict com distribuição por gênero, score, região, área
"""

from __future__ import annotations

import re
import pandas as pd
import numpy as np


# ─── Base de nomes para inferência de gênero ─────────────────────────────────
# Fonte: IBGE + DataSUS (nomes mais frequentes no Brasil)
# Cobertura estimada: ~65–70% dos primeiros nomes brasileiros

_FEMININOS: set[str] = {
    "ana", "maria", "julia", "juliana", "fernanda", "amanda", "gabriela", "patricia",
    "camila", "aline", "beatriz", "bruna", "leticia", "larissa", "renata", "vanessa",
    "mariana", "carolina", "carla", "sandra", "claudia", "silvana", "debora", "luciana",
    "simone", "monica", "rosana", "cristina", "adriana", "marcela", "priscila", "natalia",
    "tatiana", "bianca", "jessica", "stephanie", "sabrina", "viviane", "eliane", "rejane",
    "katia", "katiane", "raquel", "vera", "joana", "luana", "alice", "isabela", "isabel",
    "milena", "nathalia", "thais", "thaisa", "talia", "elisa", "emily", "emilly",
    "livia", "lidia", "lucia", "luiza", "luisa", "marina", "miriam", "michelly",
    "michelle", "nadine", "rafaela", "roberta", "rosa", "rosangela", "silvia",
    "solange", "soraia", "sueli", "suzana", "tamires", "tania", "tania", "valeria",
    "virginia", "waleria", "yasmin", "yara", "zelia", "zilda", "erica", "erika",
    "fabiana", "flavia", "giovana", "giovanna", "gisele", "gislaine", "grace",
    "heloisa", "ivone", "janaina", "jaqueline", "karina", "keila", "kelly", "lais",
    "laila", "leila", "leilane", "lorena", "lourdes", "mara", "margarete",
    "margarida", "marilia", "marilene", "marilza", "marisa", "marta", "meire",
    "meirelis", "nair", "nayara", "noemia", "odete", "pamela", "paula", "poliana",
    "regiane", "rita", "roseli", "rosilene", "rosilei", "selma", "sheila",
    "sonia", "suellen", "talita", "tamiris", "thamires", "valdirene", "wanessa",
    "yolanda", "zeli", "andressa", "cintia", "edna", "eliane", "emilia",
    "graziela", "ingrid", "iris", "lana", "leonora", "liliane", "lilian",
    "maisa", "natasha", "nayara", "nicoly", "pedrina", "pietra", "rebeca",
    "stephany", "thalia", "viviana", "waleska", "wendy",
}

_MASCULINOS: set[str] = {
    "carlos", "lucas", "pedro", "joao", "rafael", "rodrigo", "marcelo", "paulo",
    "anderson", "antonio", "andre", "alex", "alan", "daniel", "diego", "eduardo",
    "fabio", "felipe", "fernando", "francisco", "gabriel", "gilberto", "gustavo",
    "henrique", "hugo", "iago", "igor", "ivan", "jose", "jorge", "julio",
    "junior", "leonardo", "leandro", "luis", "luiz", "marcos", "mario", "mateus",
    "matheus", "mauro", "michael", "miguel", "murilo", "nelson", "nicolas", "nilson",
    "nilton", "otavio", "paulo", "raphael", "renan", "renato", "ricardo", "roberto",
    "rogerio", "romulo", "ronaldo", "roque", "ruan", "ryan", "samuel", "sandro",
    "sergio", "silvio", "tiago", "thiago", "vinicius", "vitor", "wagner", "walter",
    "wellington", "wendel", "weverton", "william", "wilson", "yuri",
    "adilson", "adriano", "airton", "alexsandro", "alfredo", "alisson", "aloysio",
    "alves", "amaro", "angelo", "augusto", "caio", "caioque", "cassio",
    "celso", "cesar", "cezar", "cleber", "cledson", "cleidson", "cleimar",
    "clemilton", "cleverson", "danilo", "davi", "david", "deivid", "deivison",
    "denilson", "denison", "denny", "dhonatas", "dirceu", "edimilson", "edilson",
    "edson", "elias", "erico", "everton", "ezequiel", "flavio", "geovane",
    "geovani", "gilson", "giovani", "gledson", "helton", "helio", "hernani",
    "israel", "jacson", "jackson", "jairo", "jean", "jefferson", "jhon",
    "jhonatan", "jonathan", "jonatan", "jonatas", "jordao", "kaique", "kaio",
    "kelvin", "keven", "kevin", "levi", "luan", "luciano", "luiz", "magno",
    "manoel", "manuel", "marlon", "maxwell", "maykol", "maycon", "mayke",
    "maynard", "messias", "moises", "natan", "natanael", "neymar", "nildo",
    "nilo", "noaldo", "pablo", "patrick", "petronilo", "philipi", "ramon",
    "reginaldo", "rener", "robson", "ronan", "rosendo", "rubens", "rudney",
    "sanderson", "saulo", "silas", "silvano", "sinval", "thales", "thyago",
    "tito", "ueslei", "ugo", "ulisses", "urian", "valdeci", "valmir", "vando",
    "vanderley", "vander", "vanilson", "vasco", "walderi", "walmir", "wanderson",
    "wanildo", "weverson", "witor", "wolney", "yago", "yan",
}

# Remove sobreposição (nomes usados para ambos os gêneros raramente)
_AMBIGUOS: set[str] = _FEMININOS & _MASCULINOS
_FEMININOS -= _AMBIGUOS
_MASCULINOS -= _AMBIGUOS


def _normalizar_nome(s: str) -> str:
    """Extrai e normaliza o primeiro nome para lookup."""
    import unicodedata
    s = str(s).strip().lower()
    # Remove acentos via NFD + filtra apenas ASCII
    s = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    # Pega apenas o primeiro token
    primeiro = re.split(r"[\s,]+", s)[0]
    return primeiro


def infer_gender(nome) -> str:
    """
    Infere gênero a partir do primeiro nome.
    Retorna 'Feminino', 'Masculino' ou 'Não identificado'.
    """
    if not nome or str(nome).strip() in ("", "nan", "None"):
        return "Não identificado"
    primeiro = _normalizar_nome(str(nome))
    if primeiro in _FEMININOS:
        return "Feminino"
    if primeiro in _MASCULINOS:
        return "Masculino"
    return "Não identificado"


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna _genero ao DataFrame baseado na coluna 'nome'.
    Opera inplace e retorna o df para encadeamento.
    """
    # Tenta encontrar qualquer coluna de nome disponível
    nome_col = None
    for candidate in ("nome", "nome_completo", "nome_cand", "candidato", "Nome completo", "Nome"):
        if candidate in df.columns and df[candidate].notna().any():
            # Filtra colunas que são emails ou IDs (contém @ ou são numéricas)
            sample = df[candidate].dropna().astype(str).head(5)
            if not any("@" in v or v.replace("-", "").replace(".", "").isdigit() for v in sample):
                nome_col = candidate
                break

    if nome_col is None:
        df["_genero"] = "Não identificado"
        print("   👤 Gênero: coluna 'nome' não encontrada — 100% Não identificado")
        return df

    df["_genero"] = df[nome_col].apply(infer_gender)
    n_fem  = (df["_genero"] == "Feminino").sum()
    n_masc = (df["_genero"] == "Masculino").sum()
    n_nd   = (df["_genero"] == "Não identificado").sum()
    print(f"   👤 Gênero: {n_fem:,} Feminino | {n_masc:,} Masculino | {n_nd:,} Não identificado")
    return df


# ─── Perfil dos aprovados ─────────────────────────────────────────────────────

def calcular_perfil_aprovados(df: pd.DataFrame) -> dict:
    """
    Calcula distribuição demográfica e de score dos candidatos aprovados (Contratados).

    Retorna dict com:
      genero            → {'Feminino': n, 'Masculino': n, 'Não identificado': n, pct_*}
      score             → {'media': x, 'mediana': x, 'p25': x, 'p75': x, 'distribuicao': [...]}
      area              → lista de {area, n, pct} ordenada por n desc
      filial            → lista de {filial, n, pct} ordenada por n desc
      sla_contratacao   → {'media': x, 'mediana': x, 'p25': x, 'p75': x}
      com_digai_pct     → % dos contratados que são Com DigAI
    """
    result: dict = {}

    contratados = df[df["status"] == "Contratado"].copy() if "status" in df.columns else df.copy()

    # Deduplica por email — mesma lógica de calcular_kpis() para garantir consistência.
    # Um candidato em múltiplas vagas conta apenas 1 vez como contratado.
    if "email" in contratados.columns:
        _emails_validos = contratados["email"].replace("", pd.NA).notna()
        _com_email = contratados[_emails_validos]
        _sem_email = contratados[~_emails_validos]
        contratados = pd.concat([
            _com_email.drop_duplicates(subset=["email"], keep="first"),
            _sem_email,
        ], ignore_index=True)

    n_total = len(contratados)

    if n_total == 0:
        return {"n_total": 0, "aviso": "Nenhum contratado encontrado"}

    result["n_total"] = n_total

    # ── Gênero ────────────────────────────────────────────────────────────────
    if "_genero" not in contratados.columns:
        contratados = enrich_dataframe(contratados.copy())

    genero_counts = contratados["_genero"].value_counts().to_dict()
    genero_result: dict = {}
    for g in ("Feminino", "Masculino", "Não identificado"):
        n = int(genero_counts.get(g, 0))
        genero_result[g] = n
        genero_result[f"pct_{g.lower().replace(' ', '_')}"] = (
            round(n / n_total * 100, 1) if n_total > 0 else 0.0
        )
    result["genero"] = genero_result

    # ── Score IA dos contratados ──────────────────────────────────────────────
    score_col = next((c for c in ("score_ia", "score_inicial_ia", "score") if c in contratados.columns), None)
    if score_col:
        scores = pd.to_numeric(contratados[score_col], errors="coerce").dropna()
        if len(scores) > 0:
            # Distribuição em faixas de 10 pontos
            bins = list(range(0, 101, 10))
            labels = [f"{b}-{b+10}" for b in bins[:-1]]
            faixas = pd.cut(scores, bins=bins, labels=labels, right=False, include_lowest=True)
            dist = faixas.value_counts().sort_index()
            result["score"] = {
                "media":        round(float(scores.mean()), 1),
                "mediana":      round(float(scores.median()), 1),
                "p25":          round(float(scores.quantile(0.25)), 1),
                "p75":          round(float(scores.quantile(0.75)), 1),
                "distribuicao": [
                    {"faixa": str(lbl), "n": int(cnt)}
                    for lbl, cnt in dist.items()
                ],
            }

    # ── Área ─────────────────────────────────────────────────────────────────
    for area_col in ("area", "area_cand", "cargo"):
        if area_col in contratados.columns:
            area_counts = (
                contratados[area_col]
                .dropna().astype(str).str.strip()
                .replace("", pd.NA).dropna()
                .value_counts()
            )
            if len(area_counts) > 0:
                result["area"] = [
                    {"area": str(k), "n": int(v), "pct": round(int(v) / n_total * 100, 1)}
                    for k, v in area_counts.head(20).items()
                ]
            break

    # ── Filial ────────────────────────────────────────────────────────────────
    for filial_col in ("filial", "filial_cand", "unidade"):
        if filial_col in contratados.columns:
            filial_counts = (
                contratados[filial_col]
                .dropna().astype(str).str.strip()
                .replace("", pd.NA).dropna()
                .value_counts()
            )
            if len(filial_counts) > 0:
                result["filial"] = [
                    {"filial": str(k), "n": int(v), "pct": round(int(v) / n_total * 100, 1)}
                    for k, v in filial_counts.head(20).items()
                ]
            break

    # ── SLA de contratação (cadastro → data_contratacao) ──────────────────────
    if "data_cadastro" in contratados.columns and "data_contratacao" in contratados.columns:
        def _tz_naive(s: pd.Series) -> pd.Series:
            s = pd.to_datetime(s, errors="coerce")
            return s.dt.tz_convert(None) if getattr(s.dt, "tz", None) else s

        sla_days = (
            _tz_naive(contratados["data_contratacao"]) -
            _tz_naive(contratados["data_cadastro"])
        ).dt.days
        sla_valid = sla_days[(sla_days >= 0) & sla_days.notna()]
        if len(sla_valid) > 0:
            result["sla_contratacao"] = {
                "media":   round(float(sla_valid.mean()), 1),
                "mediana": round(float(sla_valid.median()), 1),
                "p25":     round(float(sla_valid.quantile(0.25)), 1),
                "p75":     round(float(sla_valid.quantile(0.75)), 1),
                "n":       int(len(sla_valid)),
            }

    # ── % contratados Com DigAI ───────────────────────────────────────────────
    if "processo_seletivo" in contratados.columns:
        n_com = int((contratados["processo_seletivo"] == "Com DigAI").sum())
        result["com_digai_pct"] = round(n_com / n_total * 100, 1) if n_total > 0 else 0.0
        result["com_digai_n"]   = n_com
        result["sem_digai_n"]   = n_total - n_com

    return result
