from __future__ import annotations

import secrets
from collections import defaultdict
from datetime import UTC, datetime

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.models import Alert
from app.queries import QUERIES
from app.services.alerts import analyze_area
from app.services.db import fetch_rows, safe_first_number

app = FastAPI(title="Monitoramento Hospitalar")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
templates.env.cache = None

SESSIONS: dict[str, str] = {}

AREAS = ["Diretoria", "Enfermagem", "UTI", "Centro Cirúrgico", "Farmácia"]


METRIC_CATALOG: dict[str, dict[str, str | float]] = {
    "ocupacao_percent": {"label": "Ocupação geral", "unit": "%", "warn": 85, "critical": 95},
    "obitos_mes_atual": {"label": "Óbitos no mês", "unit": "", "warn": 15, "critical": 20},
    "fila_total": {"label": "Fila total", "unit": "pacientes", "warn": 30, "critical": 40},
    "tempo_medio_espera_min": {"label": "Tempo médio de espera", "unit": "min", "warn": 30, "critical": 45},
    "ocupacao_uti_percent": {"label": "Ocupação da UTI", "unit": "%", "warn": 85, "critical": 90},
    "pacientes_criticos": {"label": "Pacientes críticos", "unit": "pacientes", "warn": 12, "critical": 15},
    "cirurgias_atrasadas": {"label": "Cirurgias atrasadas", "unit": "cirurgias", "warn": 5, "critical": 8},
    "taxa_cancelamento_percent": {"label": "Taxa de cancelamento", "unit": "%", "warn": 8, "critical": 12},
    "itens_ruptura": {"label": "Itens em ruptura", "unit": "itens", "warn": 0, "critical": 1},
    "itens_alto_risco_baixo": {"label": "Itens de alto risco (baixo estoque)", "unit": "itens", "warn": 2, "critical": 3},
}



def _is_logged(request: Request) -> bool:
    sid = request.cookies.get("sid")
    return bool(sid and sid in SESSIONS)


def _mock_metrics() -> dict[str, dict]:
    return {
        "Diretoria": {"ocupacao_percent": 96.2, "obitos_mes_atual": 18},
        "Enfermagem": {"fila_total": 52, "tempo_medio_espera_min": 48},
        "UTI": {"ocupacao_uti_percent": 93.0, "pacientes_criticos": 16},
        "Centro Cirúrgico": {"cirurgias_atrasadas": 10, "taxa_cancelamento_percent": 9.5},
        "Farmácia": {"itens_ruptura": 2, "itens_alto_risco_baixo": 5},
    }

def _format_value(value: float | int | str, unit: str) -> str:
    if isinstance(value, (int, float)):
        if unit == "%":
            return f"{float(value):.1f}%"
        if float(value).is_integer():
            number = f"{int(value)}"
        else:
            number = f"{float(value):.1f}"
        return f"{number} {unit}".strip()
    return str(value)


def _status_from_threshold(value: float | int | str, warn: float | None, critical: float | None) -> str:
    if not isinstance(value, (int, float)) or warn is None or critical is None:
        return "informativo"
    current = float(value)
    if current > critical:
        return "crítico"
    if current > warn:
        return "atenção"
    return "controlado"


def _build_metrics_view(area_metrics: dict[str, float | int | str]) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    for key, value in area_metrics.items():
        meta = METRIC_CATALOG.get(key, {})
        label = str(meta.get("label", key.replace("_", " ").capitalize()))
        unit = str(meta.get("unit", ""))
        warn = meta.get("warn")
        critical = meta.get("critical")
        cards.append(
            {
                "key": key,
                "label": label,
                "value": _format_value(value, unit),
                "status": _status_from_threshold(
                    value,
                    warn if isinstance(warn, (int, float)) else None,
                    critical if isinstance(critical, (int, float)) else None,
                ),
                "thresholds": f"Atenção > {warn} | Crítico > {critical}" if warn is not None and critical is not None else "Sem limite configurado",
            }
        )
    return cards


def _build_alert_details(alert: Alert, area_metrics: dict[str, float | int | str]) -> dict[str, str]:
    title = alert.titulo.lower()
    descricao = alert.descricao.lower()
    metric_key = ""
    if "ocupação" in title or "ocupação" in descricao:
        metric_key = "ocupacao_uti_percent" if alert.area == "UTI" else "ocupacao_percent"
    elif "óbito" in title or "óbito" in descricao:
        metric_key = "obitos_mes_atual"
    elif "fila" in descricao or "sobrecarga" in title:
        metric_key = "fila_total"
    elif "espera" in descricao:
        metric_key = "tempo_medio_espera_min"
    elif "crític" in title and alert.area == "UTI":
        metric_key = "pacientes_criticos"
    elif "atras" in title:
        metric_key = "cirurgias_atrasadas"
    elif "cancel" in title:
        metric_key = "taxa_cancelamento_percent"
    elif "ruptura" in descricao:
        metric_key = "itens_ruptura"
    elif "alto risco" in title:
        metric_key = "itens_alto_risco_baixo"

    metric_label = "Contexto geral"
    metric_value = "Sem indicador relacionado"
    threshold = "Sem limite configurado"
    if metric_key and metric_key in area_metrics:
        meta = METRIC_CATALOG.get(metric_key, {})
        unit = str(meta.get("unit", ""))
        metric_label = str(meta.get("label", metric_key))
        metric_value = _format_value(area_metrics[metric_key], unit)
        warn = meta.get("warn")
        critical = meta.get("critical")
        if warn is not None and critical is not None:
            threshold = f"Atenção > {warn} | Crítico > {critical}"

    return {
        "related_metric_label": metric_label,
        "related_metric_value": metric_value,
        "related_metric_threshold": threshold,
        "priority_label": {"alta": "Alta prioridade", "media": "Prioridade média", "baixa": "Baixa prioridade"}.get(alert.prioridade, "Informativo"),
    }

def _humanize_query_name(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _load_query_results(limit_rows: int = 20) -> dict[str, list[dict[str, object]]]:
    query_results: dict[str, list[dict[str, object]]] = {}
    for area in AREAS:
        area_queries = QUERIES.get(area, {})
        area_results: list[dict[str, object]] = []
        for query_key, sql in area_queries.items():
            try:
                rows = fetch_rows(sql)
                columns = list(rows[0].keys()) if rows else []
                area_results.append(
                    {
                        "query_key": query_key,
                        "query_label": _humanize_query_name(query_key),
                        "row_count": len(rows),
                        "columns": columns,
                        "rows": rows[:limit_rows],
                        "truncated": len(rows) > limit_rows,
                        "error": None,
                    }
                )
            except Exception as exc:
                area_results.append(
                    {
                        "query_key": query_key,
                        "query_label": _humanize_query_name(query_key),
                        "row_count": 0,
                        "columns": [],
                        "rows": [],
                        "truncated": False,
                        "error": str(exc),
                    }
                )
        query_results[area] = area_results
    return query_results




def _load_metrics_from_queries() -> dict[str, dict]:
    metrics = defaultdict(dict)
    try:
        diretoria_rows = fetch_rows(QUERIES["Diretoria"]["internacao_e_altas"])
        internados = 0.0
        altas = 0.0
        for r in diretoria_rows:
            indic = str(r.get("indic", "")).lower()
            quant = float(r.get("quant", 0) or 0)
            if "internados" in indic:
                internados = quant
            elif "altas" in indic:
                altas = quant
        ocupacao = 0.0 if internados == 0 else min(100.0, (internados / max(internados + altas, 1)) * 100)
        metrics["Diretoria"]["ocupacao_percent"] = ocupacao

        obitos_rows = fetch_rows(QUERIES["Diretoria"]["obitos_mes"])
        metrics["Diretoria"]["obitos_mes_atual"] = safe_first_number(obitos_rows, "qtd_obitos")

        enf_rec = fetch_rows(QUERIES["Enfermagem"]["fila_recepcao"])
        enf_tri = fetch_rows(QUERIES["Enfermagem"]["fila_triagem"])
        fila_total = safe_first_number(enf_rec, "qtde_pacientes_fila") + safe_first_number(enf_tri, "quantidade")
        metrics["Enfermagem"]["fila_total"] = fila_total
        metrics["Enfermagem"]["tempo_medio_espera_min"] = 50 if fila_total > 30 else 25

        uti_rows = fetch_rows(QUERIES["UTI"]["internados"])
        internados_uti = safe_first_number(uti_rows, "quant")
        metrics["UTI"]["ocupacao_uti_percent"] = min(100.0, (internados_uti / 30.0) * 100)
        metrics["UTI"]["pacientes_criticos"] = round(internados_uti * 0.55)

        cc_rows = fetch_rows(QUERIES["Centro Cirúrgico"]["cirurgias_status_mes"])
        atrasadas = 0
        total = 0
        for r in cc_rows:
            total += int(r.get("qtde", 0) or 0)
            if str(r.get("tp_situacao", "")).upper() in {"A", "AT", "ATRASADA"}:
                atrasadas += int(r.get("qtde", 0) or 0)
        metrics["Centro Cirúrgico"]["cirurgias_atrasadas"] = atrasadas
        metrics["Centro Cirúrgico"]["taxa_cancelamento_percent"] = 0 if total == 0 else round((atrasadas / total) * 100, 2)

        far_rows = fetch_rows(QUERIES["Farmácia"]["medicamentos_alto_risco"])
        metrics["Farmácia"]["itens_ruptura"] = 0
        metrics["Farmácia"]["itens_alto_risco_baixo"] = len([r for r in far_rows if float(r.get("qtde", 0) or 0) < 10])
    except Exception:
        return _mock_metrics()
    return dict(metrics)


async def _active_alerts() -> list[Alert]:
    metrics_by_area = _load_metrics_from_queries()
    alerts: list[Alert] = []
    for area in AREAS:
        area_metrics = metrics_by_area.get(area, {})
        alerts.extend(await analyze_area(area, area_metrics))
    return sorted(alerts, key=lambda x: (x.prioridade, x.timestamp), reverse=True)


async def _active_alerts_from_metrics(metrics_by_area: dict[str, dict]) -> list[Alert]:
    alerts: list[Alert] = []
    for area in AREAS:
        area_metrics = metrics_by_area.get(area, {})
        alerts.extend(await analyze_area(area, area_metrics))
    return sorted(alerts, key=lambda x: (x.prioridade, x.timestamp), reverse=True)



@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    if not _is_logged(request):
        return RedirectResponse(url="/login", status_code=302)
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == settings.app_username and password == settings.app_password:
        sid = secrets.token_urlsafe(24)
        SESSIONS[sid] = username
        response = RedirectResponse(url="/dashboard", status_code=302)
        response.set_cookie("sid", sid, httponly=True)
        return response
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "error": "Usuário ou senha inválidos."},
        status_code=401,
    )


@app.get("/logout")
async def logout(request: Request):
    sid = request.cookies.get("sid")
    if sid:
        SESSIONS.pop(sid, None)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("sid")
    return response


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not _is_logged(request):
        return RedirectResponse(url="/login", status_code=302)
    metrics_by_area = _load_metrics_from_queries()
    alerts = await _active_alerts_from_metrics(metrics_by_area)
    grouped = defaultdict(list)
    for alert in alerts:
          grouped[alert.area].append(
            {
                "alert": alert,
                "details": _build_alert_details(alert, metrics_by_area.get(alert.area, {})),
            }
        )

    all_alerts_view = [
        {"alert": alert, "details": _build_alert_details(alert, metrics_by_area.get(alert.area, {}))}
        for alert in alerts
    ]
    metrics_view_by_area = {area: _build_metrics_view(metrics_by_area.get(area, {})) for area in AREAS}
    query_results_by_area = _load_query_results()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "areas": AREAS,
            "alerts_by_area": dict(grouped),
            "metrics_by_area": metrics_view_by_area,
            "query_results_by_area": query_results_by_area,
            "all_alerts": all_alerts_view,
            "generated_at": datetime.now(UTC),
        },
    )


@app.get("/api/alerts")
async def alerts_api(request: Request):
    if not _is_logged(request):
        return JSONResponse({"detail": "Não autenticado"}, status_code=401)
    alerts = await _active_alerts()
    return [a.model_dump(mode="json") for a in alerts]
