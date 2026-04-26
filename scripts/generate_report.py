"""
DigAI Reports Engine — CLI Principal

Uso:
  python generate_report.py \
    --funnel      pasta2_steps_funnel.csv \
    --candidatura pasta1_candidatura.csv \   # OU --contratacoes relatorio_contratacoes.csv
    --digai       digai_base.csv \
    --cliente "Atento" \
    --periodo "20/02 a 09/03" \
    --mensalidade 7600

  # Sem arquivo complementar (fallback por etapas + base DigAI):
  python generate_report.py \
    --funnel pasta2_steps_funnel.csv \
    --digai  digai_base.csv \
    --cliente "Atento" --periodo "20/02 a 09/03"

  # Modo legado (base unificada pré-pronta):
  python generate_report.py dados_unificados.csv \
    --cliente "Atento" --periodo "20/02 a 09/03"

Saídas geradas:
  reports/<cliente>/<periodo>/index.html   → dashboard HTML
  reports/<cliente>/<periodo>/data.json    → dados brutos
  reports/<cliente>/<periodo>/relatorio.xlsx → Excel 5 abas
"""

import sys
import json
import re
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from engine.analytics import gerar_relatorio, gerar_relatorio_from_sources
from engine.excel_gen import gerar_excel
from engine.dimensions import (
    detect_dimensions, print_dimensions,
    run_config_wizard, gerar_relatorios_segmentados,
)
from engine.excel_segmented import gerar_excel_segmentado

TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "dashboard.html"
REPORTS_DIR   = Path(__file__).parent.parent / "reports"


def slug(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text


def gerar_saidas(relatorio: dict, params: dict, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. JSON de dados ──────────────────────────────────────────────────────
    # Remove _df do JSON (é o DataFrame interno, não serializável)
    rel_for_json = {k: v for k, v in relatorio.items() if k != "_df"}
    json_path = out_dir / "data.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rel_for_json, f, ensure_ascii=False, indent=2, default=str)
    print(f"   📄 JSON: {json_path}")

    # ── 2. HTML Dashboard ─────────────────────────────────────────────────────
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    report_json = json.dumps(rel_for_json, ensure_ascii=False, default=str)
    html = template
    html = html.replace("{{CLIENTE}}", relatorio["meta"]["cliente"])
    html = html.replace("{{PERIODO}}", relatorio["meta"]["periodo"])
    html = html.replace("{{REPORT_JSON}}", report_json)
    html_path = out_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"   🌐 Dashboard: {html_path}")

    # ── 3. Excel ──────────────────────────────────────────────────────────────
    xlsx_path = str(out_dir / "relatorio.xlsx")
    try:
        gerar_excel(relatorio, params, xlsx_path)
        print(f"   📊 Excel: {xlsx_path}")
    except Exception as e:
        print(f"   ⚠️  Excel falhou: {e}")
        xlsx_path = None

    return {
        "html": html_path,
        "json": json_path,
        "xlsx": xlsx_path,
    }


def print_header(params: dict):
    print(f"\n{'='*60}")
    print(f"  DigAI Reports Engine")
    print(f"{'='*60}")
    print(f"  Cliente:     {params.get('cliente_nome', '?')}")
    print(f"  Período:     {params.get('periodo', '?')}")
    print(f"  Mensalidade: R$ {params.get('mensalidade_digai', '?')}")
    print(f"{'='*60}\n")


def print_results(relatorio: dict, outputs: dict):
    kpis = relatorio["kpis"]
    roi  = relatorio["roi"]
    ins  = relatorio["insights"]

    com = kpis.get("Com DigAI", {})
    sem = kpis.get("Sem DigAI", {})

    print(f"\n{'─'*60}")
    print(f"  📊 KPIs")
    print(f"{'─'*60}")
    print(f"  Candidatos:   {com.get('total',0):,} (Com) vs {sem.get('total',0):,} (Sem)")
    print(f"  Contratações: {com.get('contratados',0):,} (Com) vs {sem.get('contratados',0):,} (Sem)")
    print(f"  SLA médio:    {com.get('sla_media','—')} dias (Com) vs {sem.get('sla_media','—')} dias (Sem)")
    print(f"\n  💰 ROI: {roi.get('roi',0):.0f}x  |  Saving: R$ {roi.get('savings',0):,.2f}")

    print(f"\n{'─'*60}")
    print(f"  VEREDICTO: {ins['veredicto']}")
    print(f"{'─'*60}")
    for p in ins.get("pontos_positivos", []):
        print(f"  ✅ {p}")
    for p in ins.get("pontos_atencao", []):
        print(f"  ⚠️  {p}")

    print(f"\n{'='*60}")
    print(f"  📁 Saídas geradas:")
    if outputs.get("html"):
        print(f"     🌐 file://{outputs['html'].resolve()}")
    if outputs.get("xlsx"):
        print(f"     📊 {outputs['xlsx']}")
    if outputs.get("json"):
        print(f"     📄 {outputs['json']}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="DigAI Reports Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Modo novo: 3 fontes
    parser.add_argument("--funnel",        help="Gupy Steps Funnel Report (CSV/XLSX)")
    parser.add_argument("--candidatura",   help="Gupy Relatório de Candidatura (CSV/XLSX)")
    parser.add_argument("--contratacoes",  help="Gupy Relatório de Contratações — alternativa ao --candidatura (CSV/XLSX)")
    parser.add_argument("--digai",         help="DigAI Interview Base (CSV/XLSX)")

    # Modo legado: base unificada
    parser.add_argument("data_path", nargs="?", help="[LEGADO] Arquivo CSV unificado")

    # Segmentação
    parser.add_argument("--segmentar", action="store_true",
                        help="Ativa wizard de segmentação por dimensão")
    parser.add_argument("--dimensao",  default=None,
                        help="Dimensão para segmentar (ex: area, filial, recrutador)")
    parser.add_argument("--segmentos", default=None,
                        help="Valores separados por vírgula (ex: 'SAC,Retenção')")

    # Parâmetros do relatório
    parser.add_argument("--cliente",      default="Cliente",  help="Nome do cliente")
    parser.add_argument("--periodo",      default="",         help="Período do relatório")
    parser.add_argument("--mensalidade",  default=7600.0,  type=float, help="Mensalidade DigAI (R$)")
    parser.add_argument("--salario-ta",   default=4750.0,  type=float, help="Salário TA CLT (R$)")
    parser.add_argument("--tempo-ei",     default=30,      type=int,   help="Duração EI presencial (min)")
    parser.add_argument("--produtividade",default=0.60,    type=float, help="Produtividade do recrutador (0-1)")
    parser.add_argument("--max-ta",       default=127,     type=int,   help="Cap. máx. entrevistas TA/mês")
    parser.add_argument("--logo",         default="",                  help="URL ou path do logo do cliente")

    args = parser.parse_args()

    params = {
        "cliente_nome":         args.cliente,
        "periodo":              args.periodo,
        "mensalidade_digai":    args.mensalidade,
        "salario_ta_clt":       args.salario_ta,
        "tempo_entrevista_min": args.tempo_ei,
        "produtividade_pct":    args.produtividade,
        "max_entrevistas_ta":   args.max_ta,
        "logo_url":             args.logo,
    }

    print_header(params)

    # ── Escolhe pipeline ──────────────────────────────────────────────────────
    if args.funnel:
        # --contratacoes é alias de --candidatura; --candidatura tem precedência
        complementar_path = args.candidatura or args.contratacoes
        if args.contratacoes and not args.candidatura:
            print("   ℹ️  Usando relatório de contratações como arquivo complementar.")
        elif not complementar_path:
            print("   ℹ️  Sem arquivo complementar — contratados inferidos por etapas + base DigAI.")

        print("⏳ Pipeline novo (3 fontes)...")
        relatorio = gerar_relatorio_from_sources(
            funnel_path=args.funnel,
            candidatura_path=complementar_path,
            digai_path=args.digai,
            params=params,
        )
    elif args.data_path:
        print("⏳ Pipeline legado (base unificada)...")
        relatorio = gerar_relatorio(args.data_path, params)
        relatorio["_df"] = None  # gerar_excel requer _df; passa None p/ pular Excel
    else:
        print("❌ Erro: informe --funnel ou um arquivo de dados.")
        parser.print_help()
        sys.exit(1)

    # ── Extrai o DataFrame interno para wizard ────────────────────────────────
    df = relatorio.get("_df")

    # ── Wizard de segmentação ─────────────────────────────────────────────────
    config = None
    if df is not None and (args.segmentar or args.dimensao):
        if args.dimensao:
            # Modo não-interativo: dimensão especificada via CLI
            from engine.dimensions import detect_dimensions, filter_by_segment
            dims = detect_dimensions(df)
            dim_info = dims.get(args.dimensao)
            if dim_info is None:
                # Tenta encontrar a coluna diretamente
                if args.dimensao in df.columns:
                    dim_info = {"col": args.dimensao,
                                "values": df[args.dimensao].dropna().unique().tolist()}
            if dim_info:
                segs = (args.segmentos.split(",") if args.segmentos
                        else dim_info["values"])
                config = {
                    "mode": "segmentado",
                    "dimension": args.dimensao,
                    "dim_col": dim_info["col"],
                    "segments": segs,
                    "output_type": "single_file",
                }
        elif args.segmentar:
            config = run_config_wizard(df, params)

    # ── Gera saídas ───────────────────────────────────────────────────────────
    out_dir = REPORTS_DIR / slug(params["cliente_nome"]) / slug(params["periodo"] or "relatorio")

    if config and config["mode"] in ("segmentado", "ambos"):
        print(f"\n🔀 Gerando relatório segmentado por '{config['dimension']}'...")
        seg_results = gerar_relatorios_segmentados(df, config, params, str(out_dir))

        # Excel segmentado
        xlsx_seg = str(out_dir / "relatorio_segmentado.xlsx")
        out_dir.mkdir(parents=True, exist_ok=True)
        gerar_excel_segmentado(seg_results, config, params, xlsx_seg)
        print(f"   📊 Excel segmentado: {xlsx_seg}")

        if config["mode"] == "ambos":
            outputs = gerar_saidas(relatorio, params, out_dir)
            print_results(relatorio, outputs)
    else:
        outputs = gerar_saidas(relatorio, params, out_dir)
        print_results(relatorio, outputs)


if __name__ == "__main__":
    main()
