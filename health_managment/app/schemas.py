from typing import List, Optional
from pydantic import BaseModel, Field


class MetricData(BaseModel):
    # wspólne
    date: Optional[str] = None
    qty: Optional[float] = None
    source: Optional[str] = None

    # sleep_analysis
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    value: Optional[str] = None

    # heart_rate / vo2 / hrv
    Avg: Optional[float] = None
    Min: Optional[float] = None
    Max: Optional[float] = None
    context: Optional[str] = None


class Metric(BaseModel):
    name: str
    data: List[MetricData] = Field(default_factory=list)


class MetricsBody(BaseModel):
    metrics: List[Metric] = Field(default_factory=list)


class RootPayload(BaseModel):
    """
    Oczekiwany JSON z POST /health_metric:

    {
      "data": {
        "metrics": [
          { "name": "sleep_analysis", "data": [...] },
          { "name": "vo2_max", "data": [...] },
          ...
        ]
      }
    }
    """
    data: MetricsBody