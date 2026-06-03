"""Tests for ensemble divergence detection logic."""

from __future__ import annotations

import pytest

from topological_convergence_router.core_logic.multi_node_scanner import (
    compute_ensemble_divergence,
    _extract_model_probabilities,
)
from topological_convergence_router.models.convergence_opportunity import (
    ConvergenceOpportunity,
    EnsembleLeg,
    ConvergenceQueue,
    NODE_CLASS_PRIORITY,
)


class TestComputeEnsembleDivergence:
    def test_identical_probs_give_zero_spread(self):
        probs = {"model_a": 0.5, "model_b": 0.5, "model_c": 0.5}
        stats = compute_ensemble_divergence(probs)
        assert stats["spread"] == 0.0
        assert stats["std_dev"] == 0.0

    def test_spread_equals_max_minus_min(self):
        probs = {"model_a": 0.1, "model_b": 0.9}
        stats = compute_ensemble_divergence(probs)
        assert abs(stats["spread"] - 0.8) < 1e-9

    def test_mean_is_correct(self):
        probs = {"model_a": 0.2, "model_b": 0.8}
        stats = compute_ensemble_divergence(probs)
        assert abs(stats["mean"] - 0.5) < 1e-9

    def test_single_model_returns_zero_spread(self):
        probs = {"model_a": 0.6}
        stats = compute_ensemble_divergence(probs)
        assert stats["spread"] == 0.0
        assert stats["n_models"] == 1

    def test_empty_dict_returns_zero(self):
        stats = compute_ensemble_divergence({})
        assert stats["spread"] == 0.0

    def test_cv_zero_when_mean_near_zero(self):
        probs = {"model_a": 0.001, "model_b": 0.001}
        stats = compute_ensemble_divergence(probs)
        assert stats["cv"] == 0.0

    def test_std_dev_positive_for_different_probs(self):
        probs = {"model_a": 0.3, "model_b": 0.7, "model_c": 0.5}
        stats = compute_ensemble_divergence(probs)
        assert stats["std_dev"] > 0.0

    def test_all_keys_present(self):
        probs = {"a": 0.3, "b": 0.7}
        stats = compute_ensemble_divergence(probs)
        for key in ("spread", "mean", "std_dev", "max_spread", "n_models", "cv"):
            assert key in stats


class TestExtractModelProbabilities:
    def _make_data(self, probs_by_model: dict[str, list[float]]) -> dict:
        time_list = [f"2024-01-01T{h:02d}:00" for h in range(len(next(iter(probs_by_model.values()))))]
        hourly = {"time": time_list}
        for model, values in probs_by_model.items():
            hourly[f"precipitation_probability_{model}"] = values
        return {"hourly": hourly}

    def test_extracts_correct_hour(self):
        data = self._make_data({
            "icon_seamless": [10.0, 20.0, 30.0],
            "gfs_seamless":  [15.0, 25.0, 35.0],
        })
        probs = _extract_model_probabilities(data, forecast_hour=1)
        assert abs(probs["icon_seamless"] - 0.20) < 1e-9
        assert abs(probs["gfs_seamless"] - 0.25) < 1e-9

    def test_skips_none_values(self):
        data = self._make_data({
            "icon_seamless": [None, 20.0],
            "gfs_seamless":  [15.0, 25.0],
        })
        probs = _extract_model_probabilities(data, forecast_hour=0)
        assert "icon_seamless" not in probs
        assert "gfs_seamless" in probs

    def test_out_of_range_hour_uses_last(self):
        data = self._make_data({
            "icon_seamless": [10.0, 50.0],
        })
        probs = _extract_model_probabilities(data, forecast_hour=100)
        assert "icon_seamless" in probs


class TestConvergenceQueue:
    def test_higher_priority_dequeues_first(self):
        import asyncio

        async def run():
            q = ConvergenceQueue()
            low = ConvergenceOpportunity(
                node_class="SYNC_NODE", kind="TEST", legs=[],
                ensemble_spread=0.3, probability_sum=1.1, divergence_score=0.5,
            )
            high = ConvergenceOpportunity(
                node_class="ROUTER_NODE", kind="TEST", legs=[],
                ensemble_spread=0.3, probability_sum=1.1, divergence_score=0.5,
            )
            await q.put(low)
            await q.put(high)
            first = await q.get()
            second = await q.get()
            return first.node_class, second.node_class

        first, second = asyncio.run(run())
        assert first == "ROUTER_NODE"
        assert second == "SYNC_NODE"

    def test_qsize_tracks_correctly(self):
        import asyncio

        async def run():
            q = ConvergenceQueue()
            assert q.qsize() == 0
            opp = ConvergenceOpportunity(
                node_class="ORACLE_NODE", kind="TEST", legs=[],
                ensemble_spread=0.2, probability_sum=1.05, divergence_score=0.3,
            )
            await q.put(opp)
            assert q.qsize() == 1
            await q.get()
            assert q.qsize() == 0

        asyncio.run(run())

    def test_priority_values_are_ordered(self):
        assert NODE_CLASS_PRIORITY["ROUTER_NODE"] < NODE_CLASS_PRIORITY["RESOLVER_NODE"]
        assert NODE_CLASS_PRIORITY["RESOLVER_NODE"] < NODE_CLASS_PRIORITY["ORACLE_NODE"]
        assert NODE_CLASS_PRIORITY["ORACLE_NODE"] < NODE_CLASS_PRIORITY["SYNC_NODE"]
