"""Late-state scanner — focuses on nodes near forecast expiry where spread is highest."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from ..models.convergence_opportunity import ConvergenceOpportunity
from .multi_node_scanner import scan_node, DEFAULT_NODES

logger = logging.getLogger(__name__)

# At short forecast horizons, ensemble spread tends to be lower but more actionable
LATE_STATE_FORECAST_HOURS = [1, 2, 3, 6]
# Higher divergence threshold for late-state (short-horizon uncertainty is more informative)
LATE_STATE_DIVERGENCE_THRESHOLD = 0.20


async def scan_late_state_nodes(
    nodes: list[tuple[float, float, str]] = DEFAULT_NODES,
    *,
    divergence_threshold: float = LATE_STATE_DIVERGENCE_THRESHOLD,
    concurrency: int = 4,
) -> list[ConvergenceOpportunity]:
    """Scan nodes at multiple short-horizon forecast hours for high-stakes divergences.

    Short-horizon forecasts (1-6h) have lower uncertainty overall but when spread
    IS high at this range, it indicates genuine disagreement — the most actionable signal.
    """
    sem = asyncio.Semaphore(concurrency)
    opportunities: list[ConvergenceOpportunity] = []

    async def _scan_one(session: aiohttp.ClientSession, lat: float, lon: float, loc_id: str, fhour: int) -> None:
        async with sem:
            result = await scan_node(
                session, lat, lon, loc_id,
                divergence_threshold=divergence_threshold,
                forecast_hour=fhour,
            )
            if result is not None:
                result_with_class = ConvergenceOpportunity(
                    node_class="RESOLVER_NODE",
                    kind="LATE_STATE_SPREAD",
                    legs=result.legs,
                    ensemble_spread=result.ensemble_spread,
                    probability_sum=result.probability_sum,
                    divergence_score=result.divergence_score * 1.2,
                    location_id=result.location_id,
                    variable=result.variable,
                    forecast_hour=result.forecast_hour,
                    raw_snapshot={**result.raw_snapshot, "late_state": True},
                )
                opportunities.append(result_with_class)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _scan_one(session, lat, lon, loc_id, fhour)
            for lat, lon, loc_id in nodes
            for fhour in LATE_STATE_FORECAST_HOURS
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    if opportunities:
        logger.info("Late-state scanner found %d divergence(s)", len(opportunities))
    return opportunities


def rank_by_divergence(
    opportunities: list[ConvergenceOpportunity],
) -> list[ConvergenceOpportunity]:
    """Sort opportunities by divergence score descending — highest spread first."""
    return sorted(opportunities, key=lambda o: o.divergence_score, reverse=True)
