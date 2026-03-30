from __future__ import annotations

from datetime import datetime, UTC

from app.models import Alert
from app.services.ai import contextual_validate


def _new_alert(area: str, prioridade: str, titulo: str, descricao: str) -> dict:
    return {
        "area": area,
        "prioridade": prioridade,
        "titulo": titulo,
        "descricao": descricao,
        "timestamp": datetime.now(UTC),
    }


async def analyze_area(area: str, metrics: dict) -> list[Alert]:
    alerts: list[dict] = []

    if area == "Diretoria":
        ocupacao = float(metrics.get("ocupacao_percent", 0))
        obitos = float(metrics.get("obitos_mes_atual", 0))
        if ocupacao > 95:
            alerts.append(_new_alert(area, "alta", "Capacidade crítica", f"Ocupação em {ocupacao:.1f}%"))
        if obitos > 20:
            alerts.append(_new_alert(area, "media", "Óbitos acima do esperado", f"Óbitos no mês: {obitos:.0f}"))

    elif area == "Enfermagem":
        fila_total = float(metrics.get("fila_total", 0))
        tempo_espera = float(metrics.get("tempo_medio_espera_min", 0))
        if fila_total > 40:
            alerts.append(_new_alert(area, "alta", "Sobrecarga operacional", f"Fila atual com {fila_total:.0f} pacientes"))
        if tempo_espera > 45:
            alerts.append(_new_alert(area, "media", "Espera elevada", f"Tempo médio de espera: {tempo_espera:.0f} min"))

    elif area == "UTI":
        ocupacao_uti = float(metrics.get("ocupacao_uti_percent", 0))
        pacientes_criticos = float(metrics.get("pacientes_criticos", 0))
        if ocupacao_uti > 90:
            alerts.append(_new_alert(area, "alta", "UTI em lotação", f"Ocupação UTI em {ocupacao_uti:.1f}%"))
        if pacientes_criticos > 15:
            alerts.append(_new_alert(area, "alta", "Alta criticidade clínica", f"Pacientes críticos: {pacientes_criticos:.0f}"))

    elif area == "Centro Cirúrgico":
        atrasos = float(metrics.get("cirurgias_atrasadas", 0))
        taxa_cancel = float(metrics.get("taxa_cancelamento_percent", 0))
        if atrasos > 8:
            alerts.append(_new_alert(area, "media", "Atrasos cirúrgicos", f"{atrasos:.0f} cirurgias com atraso"))
        if taxa_cancel > 12:
            alerts.append(_new_alert(area, "alta", "Cancelamentos elevados", f"Taxa de cancelamento em {taxa_cancel:.1f}%"))

    elif area == "Farmácia":
        ruptura = float(metrics.get("itens_ruptura", 0))
        alto_risco = float(metrics.get("itens_alto_risco_baixo", 0))
        if ruptura > 0:
            alerts.append(_new_alert(area, "alta", "Risco de falta", f"{ruptura:.0f} itens em ruptura"))
        if alto_risco > 3:
            alerts.append(_new_alert(area, "alta", "Estoque crítico de alto risco", f"{alto_risco:.0f} itens de alto risco com baixo estoque"))

    validated: list[Alert] = []
    for candidate in alerts:
        enriched = await contextual_validate(area, candidate, metrics)
        validated.append(Alert(**enriched))
    return validated
