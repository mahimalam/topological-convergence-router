"""Multi-node ensemble scanner — fetches probability forecasts from Open-Meteo ensemble API."""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models.convergence_opportunity import ConvergenceOpportunity, EnsembleLeg

logger = logging.getLogger(__name__)

OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
OPEN_METEO_MODELS = ["icon_seamless", "gfs_seamless", "ecmwf_ifs04"]

# Variables whose probabilities across models must sum to 1.0 (mutually exclusive outcomes)
PROBABILITY_VARIABLES = [
    "precipitation_probability",
    "snowfall_probability" if False else "precipitation_probability",  # placeholder
]

# Monitored geographic nodes (latitude, longitude, label)
DEFAULT_NODES = [
    (40.7128, -74.0060, "new_york"),
    (51.5074, -0.1278, "london"),
    (35.6762, 139.6503, "tokyo"),
    (48.8566, 2.3522, "paris"),
    (37.7749, -122.4194, "san_francisco"),
]


async def fetch_ensemble_probabilities(
    session: aiohttp.ClientSession,
    latitude: float,
    longitude: float,
    *,
    forecast_hours: int = 24,
    timeout: float = 15.0,
) -> Optional[dict]:
    """Fetch ensemble precipitation probability from Open-Meteo."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "precipitation_probability",
        "models": ",".join(OPEN_METEO_MODELS),
        "forecast_days": max(1, forecast_hours // 24 + 1),
        "timezone": "UTC",
    }
    try:
        async with session.get(
            OPEN_METEO_ENSEMBLE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status != 200:
                logger.warning("Open-Meteo ensemble HTTP %d for (%.2f, %.2f)", resp.status, latitude, longitude)
                return None
            return await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("Open-Meteo ensemble fetch failed: %s", exc)
        return None


def _extract_model_probabilities(data: dict, forecast_hour: int = 6) -> dict[str, float]:
    """Extract per-model precipitation probability at a given forecast hour."""
    probs: dict[str, float] = {}
    hourly = data.get("hourly", {})
    time_list = hourly.get("time", [])

    if forecast_hour >= len(time_list):
        forecast_hour = min(forecast_hour, len(time_list) - 1)

    for model in OPEN_METEO_MODELS:
        key = f"precipitation_probability_{model}"
        values = hourly.get(key, [])
        if forecast_hour < len(values) and values[forecast_hour] is not None:
            probs[model] = float(values[forecast_hour]) / 100.0

    return probs


def compute_ensemble_divergence(model_probs: dict[str, float]) -> dict:
    """Compute divergence statistics across ensemble members.

    Returns:
        dict with keys: spread, mean, std_dev, max_spread, probability_sum_deviation
    """
    if len(model_probs) < 2:
        return {"spread": 0.0, "mean": 0.0, "std_dev": 0.0, "max_spread": 0.0, "n_models": len(model_probs)}

    values = list(model_probs.values())
    mean = statistics.mean(values)
    std_dev = statistics.stdev(values) if len(values) > 1 else 0.0
    spread = max(values) - min(values)

    return {
        "spread": round(spread, 4),
        "mean": round(mean, 4),
        "std_dev": round(std_dev, 4),
        "max_spread": round(spread, 4),
        "n_models": len(values),
        "cv": round(std_dev / mean if mean > 0.01 else 0.0, 4),
    }


async def scan_node(
    session: aiohttp.ClientSession,
    latitude: float,
    longitude: float,
    location_id: str,
    *,
    divergence_threshold: float = 0.15,
    forecast_hour: int = 6,
) -> Optional[ConvergenceOpportunity]:
    """Scan a single geographic node for ensemble divergence.

    Returns a ConvergenceOpportunity if ensemble spread exceeds the threshold.
    """
    data = await fetch_ensemble_probabilities(session, latitude, longitude)
    if data is None:
        return None

    model_probs = _extract_model_probabilities(data, forecast_hour)
    if len(model_probs) < 2:
        logger.debug("Insufficient model data for %s (got %d models)", location_id, len(model_probs))
        return None

    stats = compute_ensemble_divergence(model_probs)
    spread = stats["spread"]

    if spread < divergence_threshold:
        return None

    legs = [
        EnsembleLeg(
            model_id=model,
            variable="precipitation_probability",
            location_id=location_id,
            probability=prob,
            forecast_hour=forecast_hour,
        )
        for model, prob in model_probs.items()
    ]

    divergence_score = spread / max(stats["mean"], 0.01)

    logger.info(
        "Divergence detected at %s: spread=%.3f mean=%.3f cv=%.3f (hour+%d)",
        location_id, spread, stats["mean"], stats["cv"], forecast_hour,
    )

    return ConvergenceOpportunity(
        node_class="ROUTER_NODE",
        kind="ENSEMBLE_SPREAD",
        legs=legs,
        ensemble_spread=round(spread, 4),
        probability_sum=round(sum(model_probs.values()), 4),
        divergence_score=round(divergence_score, 4),
        location_id=location_id,
        variable="precipitation_probability",
        forecast_hour=forecast_hour,
        raw_snapshot={
            "latitude": latitude,
            "longitude": longitude,
            "model_probabilities": {k: round(v, 4) for k, v in model_probs.items()},
            **stats,
        },
    )


async def scan_all_nodes(
    nodes: list[tuple[float, float, str]] = DEFAULT_NODES,
    *,
    divergence_threshold: float = 0.15,
    forecast_hour: int = 6,
    concurrency: int = 5,
) -> list[ConvergenceOpportunity]:
    """Scan all geographic nodes concurrently for ensemble divergences."""
    sem = asyncio.Semaphore(concurrency)

    async def _bounded(lat, lon, loc_id):
        async with sem:
            return await scan_node(
                session, lat, lon, loc_id,
                divergence_threshold=divergence_threshold,
                forecast_hour=forecast_hour,
            )

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[_bounded(lat, lon, loc_id) for lat, lon, loc_id in nodes],
            return_exceptions=True,
        )

    opportunities = []
    for r in results:
        if isinstance(r, ConvergenceOpportunity):
            opportunities.append(r)
        elif isinstance(r, Exception):
            logger.warning("Node scan error: %s", r)
    return opportunities
