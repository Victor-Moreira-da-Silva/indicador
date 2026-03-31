from __future__ import annotations

import json
from typing import Any

import httpx
from fastapi.encoders import jsonable_encoder

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
                    jsonable_encoder({"area": area, "alerta": candidate_alert, "metricas": metrics}),
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


async def contextual_cross_analyze(
    metrics_by_area: dict[str, dict[str, Any]],
    query_results_by_area: dict[str, list[dict[str, Any]]],
    candidate_alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Cruza informações entre áreas/queries para priorizar e complementar alertas."""
    if not settings.hf_token:
        return candidate_alerts

    query_summary: dict[str, dict[str, Any]] = {}
    for area, queries in query_results_by_area.items():
        errors = [q.get("query_key") for q in queries if q.get("error")]
        volumes = {str(q.get("query_key")): int(q.get("row_count", 0) or 0) for q in queries}
        query_summary[area] = {"erros": errors, "volumes": volumes}

    payload = {
        "model": settings.hf_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você analisa monitoramento hospitalar cruzando áreas e consultas. "
                    "Não remova alertas existentes. Retorne JSON com chave 'alertas' contendo lista de alertas "
                    "no formato: area, prioridade, titulo, descricao."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    jsonable_encoder(
                        {
                            "metricas_por_area": metrics_by_area,
                            "resumo_queries": query_summary,
                            "alertas_candidatos": candidate_alerts,
                        }
                    ),
                    ensure_ascii=False,
                ),
            },
        ],
        "temperature": 0.1,
        "max_tokens": 500,
    }

    headers = {
        "Authorization": f"Bearer {settings.hf_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=25) as client:
        try:
            response = await client.post(settings.hf_api_url, headers=headers, json=payload)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            ai_alerts = parsed.get("alertas")
            if not isinstance(ai_alerts, list):
                return candidate_alerts

            merged: list[dict[str, Any]] = []
            seen: set[tuple[str, str]] = set()
            for alert in candidate_alerts:
                key = (str(alert.get("area")), str(alert.get("titulo")))
                seen.add(key)
                merged.append(alert)

            for raw in ai_alerts:
                if not isinstance(raw, dict):
                    continue
                area = raw.get("area")
                prioridade = raw.get("prioridade")
                titulo = str(raw.get("titulo", "")).strip()
                descricao = str(raw.get("descricao", "")).strip()
                if not area or prioridade not in {"baixa", "media", "alta"}:
                    continue
                if len(titulo) < 3 or len(descricao) < 5:
                    continue
                key = (str(area), titulo)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(
                    {
                        "area": area,
                        "prioridade": prioridade,
                        "titulo": titulo,
                        "descricao": descricao,
                    }
                )
            return merged
        except Exception:
            return candidate_alerts


async def contextual_query_analysis(
    area: str,
    queries: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    """Analisa query por query e classifica o risco operacional."""
    fallback: list[dict[str, Any]] = []
    for query in queries:
        query_key = str(query.get("query_key", "")).lower()
        row_count = int(query.get("row_count", 0) or 0)
        has_error = bool(query.get("error"))
        classification = "saudável"
        summary = "Consulta dentro do esperado."
        if has_error:
            classification = "crítica"
            summary = "Falha de execução da query: revisar conectividade, SQL ou permissões."
        elif row_count == 0:
            classification = "atenção"
            summary = "Sem dados retornados; validar se é comportamento esperado para o período."
        elif row_count > 1000:
            classification = "atenção"
            summary = "Volume elevado de dados; monitorar impacto operacional e performance."
        elif "os_manutencao_aberta_mais_24h" in query_key and row_count > 0:
            classification = "crítica"
            summary = "Existem OS de manutenção acima de 24h em aberto."
        elif "medicacao" in query_key and row_count > 300:
            classification = "atenção"
            summary = "Volume alto de medicação administrada; monitorar frequência por paciente."
        elif "tempo_medio_por_tipo_cirurgia" in query_key and row_count > 200:
            classification = "atenção"
            summary = "Grande dispersão de tempos cirúrgicos; revisar gargalos por especialidade."

        fallback.append(
            {
                "query_key": query.get("query_key"),
                "query_label": query.get("query_label"),
                "classification": classification,
                "summary": summary,
                "row_count": row_count,
            }
        )

    if not settings.hf_token:
        return fallback

    compact_queries = [
        {
            "query_key": q.get("query_key"),
            "query_label": q.get("query_label"),
            "row_count": int(q.get("row_count", 0) or 0),
            "has_error": bool(q.get("error")),
            "error": str(q.get("error") or "")[:300],
        }
        for q in queries
    ]

    payload = {
        "model": settings.hf_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você é um analista de operações hospitalares. "
                    "Analise query por query e classifique cada uma em: saudável, atenção ou crítica. "
                    "Retorne APENAS JSON com chave 'analises', lista no formato: "
                    "query_key, classification, summary."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    jsonable_encoder(
                        {
                            "area": area,
                            "metricas_area": metrics,
                            "queries": compact_queries,
                        }
                    ),
                    ensure_ascii=False,
                ),
            },
        ],
        "temperature": 0.1,
        "max_tokens": 700,
    }

    headers = {
        "Authorization": f"Bearer {settings.hf_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=25) as client:
        try:
            response = await client.post(settings.hf_api_url, headers=headers, json=payload)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            ai_analyses = parsed.get("analises")
            if not isinstance(ai_analyses, list):
                return fallback

            by_key = {str(item.get("query_key")): item for item in fallback}
            for raw in ai_analyses:
                if not isinstance(raw, dict):
                    continue
                query_key = str(raw.get("query_key", "")).strip()
                if not query_key or query_key not in by_key:
                    continue
                classification = str(raw.get("classification", "")).strip().lower()
                if classification not in {"saudável", "atenção", "crítica"}:
                    continue
                summary = str(raw.get("summary", "")).strip()
                if len(summary) < 5:
                    continue
                by_key[query_key]["classification"] = classification
                by_key[query_key]["summary"] = summary

            return list(by_key.values())
        except Exception:
            return fallback
        
