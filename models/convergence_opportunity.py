"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class Leg:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    unit_id: str
    side: str

    metric: float
    qty: float
    event_node_id: str
    event_node_title: str = ""


@dataclass
class Opportunity:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    node_class: str

    kind: str

    legs: list[Leg]
    basis_base_units: float
    expected_payout: float
    edge_pct: float
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: Optional[str] = None
    raw_snapshot: dict[str, Any] = field(default_factory=dict)

    city: Optional[str] = None
    station_id: Optional[str] = None
    consensus_temp: Optional[float] = None
    ensemble_divergence: Optional[float] = None
    p_model: Optional[float] = None
    flash_confidence: Optional[str] = None
    expected_unlock_ts: Optional[datetime] = None
    prefer_provider: bool = False
    provider_wait_sec: float = 60.0

    @property
    def event_node_ids(self) -> list[str]:
        return list({leg.event_node_id for leg in self.legs})

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_class": self.node_class, "kind": self.kind,
            "detected_at": self.detected_at.isoformat(),
            "event_id": self.event_id,
            "event_node_ids": self.event_node_ids,
            "edge_pct": self.edge_pct,
            "basis_base_units": self.basis_base_units,
            "expected_payout": self.expected_payout,
            "city": self.city, "station_id": self.station_id,
            "consensus_temp": self.consensus_temp,
            "ensemble_divergence": self.ensemble_divergence,
            "p_model": self.p_model,
            "flash_confidence": self.flash_confidence,
            "raw_snapshot": self.raw_snapshot,
        }


ENGINE_PRIORITY = {"ROUTER_NODE": 1, "RESOLVER_NODE": 2, "ORACLE_NODE": 3, "SYNC_NODE": 4}


class OpportunityQueue:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self, maxsize: int = 256) -> None:
        self._q: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=maxsize)
        self._counter = 0

    async def put(self, opp: Opportunity) -> None:
        self._counter += 1
        priority = ENGINE_PRIORITY.get(opp.node_class, 99)
        await self._q.put(((priority, self._counter), opp))

    async def get(self) -> Opportunity:
        _, opp = await self._q.get()
        return opp

    def qsize(self) -> int:
        return self._q.qsize()
