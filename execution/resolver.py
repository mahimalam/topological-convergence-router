"""Convergence resolver — executes multi-leg resolution vectors asynchronously."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional

from ..config.settings import CONFIG
from ..models.convergence_opportunity import ConvergenceOpportunity

logger = logging.getLogger(__name__)


@dataclass
class ResolutionResult:
    """Outcome of a convergence resolution attempt."""
    opportunity_id: str
    location_id: Optional[str]
    kind: str
    resolved: bool
    divergence_score: float
    ensemble_spread: float
    latency_ms: float
    resolved_at: datetime
    error: Optional[str] = None


class ConvergenceResolver:
    """Asynchronously resolves topological divergences across multiple nodes.

    Executes resolution as close to simultaneously as possible across all legs
    to minimize exposure to partial state shifts.
    """

    def __init__(self) -> None:
        self._resolved_count = 0
        self._failed_count = 0

    async def resolve(
        self,
        opportunity: ConvergenceOpportunity,
    ) -> Optional[ResolutionResult]:
        """Resolve a detected convergence opportunity within the configured timeout."""
        t0 = time.monotonic()
        timeout_sec = CONFIG.execution_timeout_ms / 1000.0

        try:
            result = await asyncio.wait_for(
                self._execute_resolution(opportunity),
                timeout=timeout_sec,
            )
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            self._resolved_count += 1
            logger.info(
                "Resolved %s at %s: spread=%.3f score=%.3f in %.1fms",
                opportunity.kind,
                opportunity.location_id,
                opportunity.ensemble_spread,
                opportunity.divergence_score,
                elapsed_ms,
            )
            result.latency_ms = round(elapsed_ms, 2)
            return result
        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            self._failed_count += 1
            logger.warning(
                "Resolution timeout for %s at %s after %.1fms",
                opportunity.kind, opportunity.location_id, elapsed_ms,
            )
            return ResolutionResult(
                opportunity_id=f"{opportunity.location_id}_{opportunity.forecast_hour}",
                location_id=opportunity.location_id,
                kind=opportunity.kind,
                resolved=False,
                divergence_score=opportunity.divergence_score,
                ensemble_spread=opportunity.ensemble_spread,
                latency_ms=round(elapsed_ms, 2),
                resolved_at=datetime.now(timezone.utc),
                error="timeout",
            )
        except Exception as exc:
            self._failed_count += 1
            logger.error("Resolution error: %s", exc)
            return None

    async def _execute_resolution(
        self,
        opportunity: ConvergenceOpportunity,
    ) -> ResolutionResult:
        """Execute multi-leg resolution as close to simultaneously as possible."""
        # Dispatch all legs concurrently — each resolves independently
        leg_tasks = [
            self._resolve_leg(leg)
            for leg in opportunity.legs
        ]
        results = await asyncio.gather(*leg_tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        resolved = success_count == len(opportunity.legs)

        return ResolutionResult(
            opportunity_id=f"{opportunity.location_id}_{opportunity.forecast_hour}",
            location_id=opportunity.location_id,
            kind=opportunity.kind,
            resolved=resolved,
            divergence_score=opportunity.divergence_score,
            ensemble_spread=opportunity.ensemble_spread,
            latency_ms=0.0,
            resolved_at=datetime.now(timezone.utc),
        )

    async def _resolve_leg(self, leg) -> bool:
        """Resolve a single ensemble leg — validate and record the probability reading."""
        await asyncio.sleep(0)  # yield to event loop
        return 0.0 <= leg.probability <= 1.0

    @property
    def stats(self) -> dict:
        return {
            "resolved": self._resolved_count,
            "failed": self._failed_count,
        }
