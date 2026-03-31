from datetime import datetime, UTC
from typing import Literal

from pydantic import BaseModel, Field

Prioridade = Literal["baixa", "media", "alta"]
Area = Literal["Diretoria", "Enfermagem", "UTI", "Centro Cirúrgico", "Farmácia", "Operações Integradas"]


class Alert(BaseModel):
    area: Area
    prioridade: Prioridade
    titulo: str = Field(min_length=3, max_length=120)
    descricao: str = Field(min_length=5, max_length=600)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MetricSnapshot(BaseModel):
    area: Area
    values: dict[str, float | int | str]
