from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import settings


async def contextual_validate(area: str, candidate_alert: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Valida/complementa um alerta usando IA.

    A IA nunca substitui as regras: apenas ajusta prioridade/descrição
    quando houver contexto adicional.
    """
    if not settings.hf_token:
        return candidate_alert

    payload = {
        "model": settings.hf_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você valida alertas hospitalares. Nunca remova o alerta. "
                    "Responda JSON com as chaves: prioridade, descricao_contextual."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {"area": area, "alerta": candidate_alert, "metricas": metrics},
                    ensure_ascii=False,
                ),
            },
        ],
        "temperature": 0.1,
        "max_tokens": 180,
    }

    headers = {
        "Authorization": f"Bearer {settings.hf_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            response = await client.post(settings.hf_api_url, headers=headers, json=payload)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            prioridade = parsed.get("prioridade")
            if prioridade in {"baixa", "media", "alta"}:
                candidate_alert["prioridade"] = prioridade
            extra = parsed.get("descricao_contextual")
            if isinstance(extra, str) and extra.strip():
                candidate_alert["descricao"] = f"{candidate_alert['descricao']} | Contexto IA: {extra.strip()}"
            return candidate_alert
        except Exception:
            return candidate_alert
