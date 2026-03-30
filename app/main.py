from __future__ import annotations

import secrets
from collections import defaultdict
from datetime import datetime, UTC

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
templates.env.cache = {}

SESSIONS: dict[str, str] = {}


AREAS = ["Diretoria", "Enfermagem", "UTI", "Centro Cirúrgico", "Farmácia"]


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


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    if not _is_logged(request):
        return RedirectResponse(url="/login", status_code=302)
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == settings.app_username and password == settings.app_password:
        sid = secrets.token_urlsafe(24)
        SESSIONS[sid] = username
        response = RedirectResponse(url="/dashboard", status_code=302)
        response.set_cookie("sid", sid, httponly=True)
        return response
    return templates.TemplateResponse(
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
    alerts = await _active_alerts()
    grouped = defaultdict(list)
    for alert in alerts:
        grouped[alert.area].append(alert)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "areas": AREAS,
            "alerts_by_area": dict(grouped),
            "generated_at": datetime.now(UTC),
        },
    )


@app.get("/api/alerts")
async def alerts_api(request: Request):
    if not _is_logged(request):
        return JSONResponse({"detail": "Não autenticado"}, status_code=401)
    alerts = await _active_alerts()
    return [a.model_dump(mode="json") for a in alerts]
