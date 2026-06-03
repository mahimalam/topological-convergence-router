"""Data structures for ensemble divergence detection and resolution routing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class EnsembleLeg:
    """A single ensemble member's probability estimate for a forecast node."""
    model_id: str
    variable: str
    location_id: str
    probability: float
    forecast_hour: int
    retrieved_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ConvergenceOpportunity:
    """A detected topological inconsistency across ensemble members.

    When mutually exclusive forecast outcomes sum to a value significantly different
    from 1.0 (within the ensemble spread), a resolution vector is compiled.
    """
    node_class: str
    kind: str
    legs: list[EnsembleLeg]
    ensemble_spread: float
    probability_sum: float
    divergence_score: float
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    location_id: Optional[str] = None
    variable: Optional[str] = None
    forecast_hour: Optional[int] = None
    raw_snapshot: dict[str, Any] = field(default_factory=dict)

    @property
    def location_ids(self) -> list[str]:
        return list({leg.location_id for leg in self.legs})

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_class": self.node_class,
            "kind": self.kind,
            "detected_at": self.detected_at.isoformat(),
            "location_id": self.location_id,
            "variable": self.variable,
            "forecast_hour": self.forecast_hour,
            "ensemble_spread": self.ensemble_spread,
            "probability_sum": self.probability_sum,
            "divergence_score": self.divergence_score,
            "n_legs": len(self.legs),
            "raw_snapshot": self.raw_snapshot,
        }


NODE_CLASS_PRIORITY = {
    "ROUTER_NODE":   1,
    "RESOLVER_NODE": 2,
    "ORACLE_NODE":   3,
    "SYNC_NODE":     4,
}


class ConvergenceQueue:
    """Priority queue for convergence opportunities ordered by node class."""

    def __init__(self, maxsize: int = 256) -> None:
        self._q: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=maxsize)
        self._counter = 0

    async def put(self, opp: ConvergenceOpportunity) -> None:
        self._counter += 1
        priority = NODE_CLASS_PRIORITY.get(opp.node_class, 99)
        await self._q.put(((priority, self._counter), opp))

    async def get(self) -> ConvergenceOpportunity:
        _, opp = await self._q.get()
        return opp

    def qsize(self) -> int:
        return self._q.qsize()
