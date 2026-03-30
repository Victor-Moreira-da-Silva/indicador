# Indicador Hospitalar em Tempo Real

Sistema de monitoramento hospitalar em tempo real com análise por setor e exibição apenas de alertas relevantes para tomada de decisão.

## Funcionalidades

- Login simples com sessão.
- Painel com abas HTML por área:
  - Diretoria
  - Enfermagem
  - UTI
  - Centro cirúrgico
  - Farmácia
- Backend em Python (FastAPI).
- Pipeline de alerta híbrido:
  - Regras fixas (limites críticos)
  - IA (Hugging Face) para validar/complementar contexto
- Contrato único de alertas:
  - `area`
  - `prioridade` (`baixa`, `media`, `alta`)
  - `titulo`
  - `descricao`
  - `timestamp`
- Oracle DB com queries fornecidas pelo projeto.

## Execução

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Acesse: `http://127.0.0.1:8000`

## Variáveis de ambiente

Veja `.env.example` para configurações de Oracle, IA e autenticação.
