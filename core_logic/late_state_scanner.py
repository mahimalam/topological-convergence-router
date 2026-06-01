"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

from ..common import net_circuit
from ..common.gas_costs import net_edge_pct
from ..common.net_errors import Backoff, is_network_error
from ..config import CONFIG, ENV
from .. import db as _db
from ..execution.allocation_oracle import virtual_paper_allocation, base_unitsc_allocation
from ..ingestion.binance_ws import PrimarySourceTickGasCostd, fetch_kline_open
from ..ingestion.chainlink_gas_costd import fetch_chainlink_metric, get_latest_chainlink_metric
from ..ingestion.network_client import NetworkClient, PayloadBook
from ..ingestion.gamma_client import GammaClient, GammaEventNode
from ..ingestion.public_sentiment_node_book_ws import PublicSentimentNodeBookWS
from ..ingestion.metric_consensus import MetricConsensus
from .opportunity import Leg, Opportunity

MetricGasCostd = object

logger = logging.getLogger(__name__)

_chainlink_cache: dict[str, tuple[float, float]] = {}
_CHAINLINK_MAX_AGE_SEC = 60.0

_unit_to_asset_direction: dict[str, tuple[str, str]] = {}



@dataclass
class _EngineState:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    consecutive_distributed_computecites: int = 0
    last_distributed_computecit_at: float = 0.0
    paused_until_ts: float = 0.0

    cumulative_distributed_computecit_base_units: float = 0.0
    recent_distributed_computecit_until: dict[str, float] = field(default_factory=dict)
    asset_direction_distributed_computecit_until: dict[tuple[str, str], float] = field(default_factory=dict)
    tier_paused_until: dict[str, float] = field(default_factory=dict)
    _tier_check_cache: dict[str, tuple[float, bool]] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record_result(
        self, realized_delta: float, *, unit_id: Optional[str] = None,
        cooldown_sec: int = 1800,
    ) -> None:
        async with self._lock:
            if realized_delta < 0:
                self.consecutive_distributed_computecites += 1
                self.last_distributed_computecit_at = time.monotonic()
                self.cumulative_distributed_computecit_base_units += abs(realized_delta)
                if unit_id:
                    self.recent_distributed_computecit_until[unit_id] = time.time() + cooldown_sec
                    ad_key = _unit_to_asset_direction.get(unit_id)
                    if ad_key is not None:
                        self.asset_direction_distributed_computecit_until[ad_key] = time.time() + cooldown_sec
            else:
                self.consecutive_distributed_computecites = 0
                self.cumulative_distributed_computecit_base_units = 0.0

    def is_event_node_in_cooldown(self, unit_id: str) -> bool:
        now = time.time()
        until = self.recent_distributed_computecit_until.get(unit_id, 0.0)
        if until > now:
            return True
        self.recent_distributed_computecit_until.pop(unit_id, None)
        ad_key = _unit_to_asset_direction.get(unit_id)
        if ad_key is not None:
            ad_until = self.asset_direction_distributed_computecit_until.get(ad_key, 0.0)
            if ad_until > now:
                return True
            self.asset_direction_distributed_computecit_until.pop(ad_key, None)
        return False

    async def is_paused(self, cfg: dict) -> bool:
        async with self._lock:
            limit = int(cfg.get("consecutive_distributed_computecit_limit", 3))
            distributed_computecit_base_units_limit = float(cfg.get("consecutive_distributed_computecit_pause_base_units", 2.0))
            sleep_min = int(cfg.get("consecutive_distributed_computecit_sleep_min", 240))
            should_pause = (
                self.consecutive_distributed_computecites >= limit
                or self.cumulative_distributed_computecit_base_units >= distributed_computecit_base_units_limit
            )
            if should_pause:
                self.paused_until_ts = max(self.paused_until_ts, time.time() + sleep_min * 60)
                self.consecutive_distributed_computecites = 0
                self.cumulative_distributed_computecit_base_units = 0.0
            return time.time() < self.paused_until_ts

    def is_tier_paused(self, tier_name: str, cfg: dict) -> bool:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        tap = cfg.get("tier_auto_pause", {})
        if not tap.get("enabled"):
            return False
        now = time.time()
        if now < self.tier_paused_until.get(tier_name, 0.0):
            return True
        interval = float(tap.get("check_interval_sec", 60))
        last_check_ts, last_paused = self._tier_check_cache.get(tier_name, (0.0, False))
        if (now - last_check_ts) < interval:
            return last_paused
        stats = _get_tier_delta(tier_name, int(tap.get("window_hours", 24)))
        min_executions = int(tap.get("min_executions", 5))
        threshold = float(tap.get("delta_pause_threshold_base_units", -0.30))
        paused = stats["n_executions"] >= min_executions and stats["delta_base_units"] < threshold
        if paused:
            self.tier_paused_until[tier_name] = now + float(tap.get("pause_duration_hours", 2)) * 3600.0
            logger.warning(
                "E3 tier %s auto-paused: n=%d, delta=$%.2f, win=%d/%d",
                tier_name, stats["n_executions"], stats["delta_base_units"],
                stats["n_wins"], stats["n_executions"],
            )
        self._tier_check_cache[tier_name] = (now, paused)
        return paused


_e3_state = _EngineState()


def _get_tier_delta(tier_name: str, window_hours: int) -> dict:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    from .. import db
    import json as _json
    out = {"n_executions": 0, "n_wins": 0, "delta_base_units": 0.0}
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT p.realized_delta, o.raw_snapshot "
                "FROM vectors p JOIN opportunities o ON o.id = p.opp_id "
                "WHERE p.engine='E3' AND p.status='CLOSED' "
                "AND p.realized_delta IS NOT NULL "
                f"AND p.resolved_at > datetime('now', '-{int(window_hours)} hours')"
            )
            for row in cur.fetchall():
                try:
                    snap = _json.loads(row["raw_snapshot"] or "{}")
                except Exception:
                    continue
                if snap.get("tier") != tier_name:
                    continue
                delta = float(row["realized_delta"] or 0.0)
                out["n_executions"] += 1
                out["delta_base_units"] += delta
                if delta > 0:
                    out["n_wins"] += 1
    except Exception as exc:
        logger.debug("E3 tier P&L query failed for %s: %s", tier_name, exc)
    return out



STRIKE_RE = re.compile(
    r"\$?(\d+(?:\.\d+)?\s*[kK]|\d{1,3}(?:,\d{3})+|\d{3,7}(?:\.\d+)?)",
)

_DIR_KEYWORDS = (
    "up or down", "above", "below", "higher", "lower", "over", "under",
    "exceed", "exceeds", "exceeding", "reach", "reaches", "hit", "hits",
    "close above", "close below", "by ", "before ", "by month end",
    "this week", "this month", "today", "tomorrow", "tonight",
)

_ASSET_KEYWORDS: dict[str, tuple[str, ...]] = {
    "BTC": ("btc", "bitcoin"),
    "ETH": ("eth", "ethereum"),
    "SOL": ("sol", "solana"),
    "XRP": ("xrp", "ripple"),
    "DOGE": ("doge", "dogecoin"),
    "BNB": ("bnb", "binance coin"),
    "HYPE": ("hype", "hyperliquid"),
}
_ASSET_WHOLE_WORD = {"eth", "sol", "btc", "xrp", "doge", "bnb", "hype"}
_WB_PATTERNS: dict[str, re.Pattern] = {
    kw: re.compile(rf"\b{re.escape(kw)}\b", re.I) for kw in _ASSET_WHOLE_WORD
}


def _has_asset_keyword(text: str, asset: str) -> bool:
    for kw in _ASSET_KEYWORDS.get(asset, ()):
        if kw in _ASSET_WHOLE_WORD:
            if _WB_PATTERNS[kw].search(text):
                return True
        elif kw in text:
            return True
    return False


def parse_strike(event_node_title: str) -> Optional[float]:
    m = STRIKE_RE.search(event_node_title)
    if not m:
        return None
    raw = m.group(1).replace(",", "").strip()
    try:
        if raw.lower().endswith("k"):
            return float(raw[:-1].strip()) * 1000.0
        return float(raw)
    except ValueError:
        return None


def is_crypto_late_event_node(event_node: GammaEventNode, asset: str) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    text = f"{event_node.question}".lower()
    if not _has_asset_keyword(text, asset):
        return False
    if not any(kw in text for kw in _DIR_KEYWORDS):
        return False
    return parse_strike(event_node.question) is not None


_UPDOWN_RE = re.compile(r"\bup\s+or\s+down\b", re.I)
_UPDOWN_TIME_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\s*ET", re.I,
)
_UPDOWN_RANGE_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*[-–]\s*(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
    re.I,
)
_UPDOWN_DURATION_RE = re.compile(r"\b(\d+)\s*(m|min|mins|h|hr|hrs)\b", re.I)


def _et_time_to_minutes(hour: int, minute: int, suffix: str) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    suffix = suffix.upper()
    if suffix == "AM" and hour == 12:
        hour = 0
    elif suffix == "PM" and hour != 12:
        hour += 12
    return hour * 60 + minute


def _updown_window(question: str) -> timedelta:
    rm = _UPDOWN_RANGE_RE.search(question or "")
    if rm:
        start_min = _et_time_to_minutes(int(rm.group(1)), int(rm.group(2)), rm.group(3))
        end_min = _et_time_to_minutes(int(rm.group(4)), int(rm.group(5)), rm.group(6))
        diff = end_min - start_min
        if diff <= 0:
            diff += 24 * 60

        return timedelta(minutes=diff)
    m = _UPDOWN_DURATION_RE.search(question or "")
    if not m:
        return timedelta(hours=1)
    n = int(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("m"):
        return timedelta(minutes=n)
    return timedelta(hours=n)


def is_updown_event_node(event_node: GammaEventNode, asset: str) -> bool:
    text = f"{event_node.question}".lower()
    if not _has_asset_keyword(text, asset):
        return False
    if not _UPDOWN_RE.search(text):
        return False
    node_states = [o.lower() for o in (event_node.node_states or [])]
    return ("up" in node_states) and ("down" in node_states)


def parse_updown_open_time(event_node: GammaEventNode) -> Optional[datetime]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    raw_events = (event_node.raw or {}).get("events") or []
    if raw_events:
        start_time_str = (raw_events[0] or {}).get("startTime")
        if start_time_str:
            try:
                st = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
                return st
            except Exception:
                pass

    if not event_node.end_date_iso:
        return None
    try:
        end = datetime.fromisoformat(event_node.end_date_iso.replace("Z", "+00:00"))
    except Exception:
        return None

    is_dst = 3 <= end.month <= 11
    offset = 4 if is_dst else 5

    rm = _UPDOWN_RANGE_RE.search(event_node.question or "")
    if rm:
        sh = int(rm.group(1)); sm = int(rm.group(2)); ss = rm.group(3).upper()
        if ss == "AM" and sh == 12:
            sh = 0
        elif ss == "PM" and sh != 12:
            sh += 12
        start_utc_hour = (sh + offset) % 24
        start_dt = end.replace(hour=start_utc_hour, minute=sm, second=0, microsecond=0)
        if start_dt >= end:
            start_dt -= timedelta(days=1)
        if timedelta(0) < (end - start_dt) <= timedelta(hours=2):
            return start_dt

    window = _updown_window(event_node.question or "")
    m = _UPDOWN_TIME_RE.search(event_node.question or "")
    if not m:
        return end - window
    hour = int(m.group(1))
    minute = int(m.group(2) or 0)
    suffix = m.group(3).upper()
    if suffix == "PM" and hour < 12:
        hour += 12
    elif suffix == "AM" and hour == 12:
        hour = 0
    strike_utc_hour = (hour + offset) % 24
    strike_dt = end.replace(hour=strike_utc_hour, minute=minute, second=0, microsecond=0)
    if strike_dt >= end:
        strike_dt -= timedelta(days=1)
    if (end - strike_dt) > window * 1.5 or strike_dt > end:
        return end - window
    return strike_dt


_ASSET_BINANCE_SYMBOL = {
    "BTC": "BTCBASE_UNITST", "ETH": "ETHBASE_UNITST", "SOL": "SOLBASE_UNITST",
    "XRP": "XRPBASE_UNITST", "DOGE": "DOGEBASE_UNITST", "BNB": "BNBBASE_UNITST",
}


def hours_to_resolution(end_iso: Optional[str]) -> float:
    if not end_iso:
        return 9999.0
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return (end - datetime.now(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        return 9999.0


def secs_to_resolution(end_iso: Optional[str]) -> float:
    return hours_to_resolution(end_iso) * 3600.0



from collections import defaultdict
_gate_counts: defaultdict[str, int] = defaultdict(int)
_gate_counts_enabled = False


@dataclass
class WatchedEventNode:
    event_node: GammaEventNode
    asset: str
    strike: float
    direction_up_node_state_idx: int
    direction_down_node_state_idx: int
    kind: str = "STRIKE"

    updown_window_sec: float = 0.0

    strike_resolved: bool = True

    added_at: float = field(default_factory=time.monotonic)
    last_book_check: float = 0.0
    last_book: Optional[PayloadBook] = None
    last_emit_at: float = 0.0
    locked_direction: Optional[str] = None

    locked_since: float = 0.0

    upper_bound_history: deque = field(default_factory=lambda: deque(maxlen=12))


def _classify_node_state_indices(event_node: GammaEventNode) -> tuple[int, int]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    node_states = [o.lower() for o in (event_node.node_states or [])]
    up_keywords = ("up", "above", "higher", "over", "yes")
    dn_keywords = ("down", "below", "lower", "under", "no")
    up_idx = next((i for i, o in enumerate(node_states)
                   if any(kw in o for kw in up_keywords)), 0)
    dn_idx = next((i for i, o in enumerate(node_states)
                   if any(kw in o for kw in dn_keywords)),
                  1 if len(node_states) > 1 else 0)
    if up_idx == dn_idx and len(node_states) > 1:
        dn_idx = 1 - up_idx
    return up_idx, dn_idx


def _tier_for(secs_remaining: float, tiers: list[dict]) -> Optional[dict]:
    for t in tiers:
        if t["min_secs"] <= secs_remaining <= t["max_secs"]:
            return t
    return None


def _adjust_win_probability(
    tier: dict, margin_bps: float, recent_move_bps: float = 0.0,
) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    base = float(tier.get("win_probability", 0.98))
    min_bps = float(tier.get("min_bps", 0.0))
    max_prob = float(tier.get("win_probability_max", min(base + 0.05, 0.995)))
    range_bps = float(tier.get("win_probability_bps_range", max(min_bps * 4.0, 20.0)))

    vol_lock_mult = float(tier.get("vol_lock_mult", 3.0))
    if recent_move_bps > 0:
        strength = min(1.0, margin_bps / max(1e-6, vol_lock_mult * recent_move_bps))
        base = 0.50 + (base - 0.50) * strength
        max_prob = 0.50 + (max_prob - 0.50) * strength

    if margin_bps <= min_bps or range_bps <= 0:
        return base
    t = min(1.0, (margin_bps - min_bps) / range_bps)
    return min(max_prob, base + (max_prob - base) * t)


def _divergence_and_depth(book: PayloadBook) -> tuple[Optional[float], float]:
    if book.best_upper_bound is None or book.best_lower_bound is None:
        return None, 0.0
    divergence = book.best_upper_bound - book.best_lower_bound
    depth_base_units = book.best_upper_bound * book.best_upper_bound_size
    return divergence, depth_base_units


def _is_mm_absent(book: PayloadBook, mm_cfg: dict) -> bool:
    divergence, depth_base_units = _divergence_and_depth(book)
    if divergence is None:
        return True
    if divergence >= mm_cfg["divergence_threshold"]:
        return True
    if depth_base_units <= mm_cfg["max_upper_bound_depth_base_units"]:
        return True
    return False




_E3_CRYPTO_TAGS = {"crypto", "crypto-metrics", "bitcoin", "ethereum", "solana"}


async def _discover_once(
    gamma: GammaClient, lookahead_sec: int,
) -> list[GammaEventNode]:
    now = datetime.now(timezone.utc)
    end_max = (now + timedelta(seconds=lookahead_sec)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_min = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        events = await gamma.list_events_all(
            active=True, closed=False, pages=20, page_size=100, concurrency=5,
            ends_after_iso=end_min, ends_before_iso=end_max,
        )
    except Exception as exc:
        logger.warning("E3 discovery list_events failed: %s", exc)
        return []
    event_nodes: list[GammaEventNode] = []
    for ev in events:
        if not (set(ev.tag_slugs) & _E3_CRYPTO_TAGS):
            continue
        event_nodes.extend(ev.event_nodes)
    return event_nodes


async def _add_to_watch(
    watch: dict[str, WatchedEventNode],
    event_node: GammaEventNode,
    asset: str,
    max_size: int,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if event_node.id in watch:
        watch[event_node.id].event_node = event_node
        return
    if not event_node.network_unit_ids or len(event_node.network_unit_ids) < 2:
        return

    kind = "STRIKE"
    strike: Optional[float] = parse_strike(event_node.question)
    strike_resolved = True

    updown_window_sec = 0.0
    if (strike is None or strike <= 0) and is_updown_event_node(event_node, asset):
        kind = "UPDOWN"
        window = _updown_window(event_node.question or "")
        updown_window_sec = window.total_seconds()
        open_time = parse_updown_open_time(event_node)
        if open_time is None:
            return
        now_utc = datetime.now(timezone.utc)
        if open_time > now_utc:
            strike = 0.0
            strike_resolved = False
        else:
            ts_ms = int(open_time.timestamp() * 1000)
            rpc_url = ENV.polygon_rpc_url
            strike_metric = await fetch_chainlink_metric(asset, ts_ms, rpc_url)
            if strike_metric is None or strike_metric <= 0:
                symbol = _ASSET_BINANCE_SYMBOL.get(asset)
                if symbol is None:
                    return
                strike_metric = await fetch_kline_open(symbol, ts_ms)
            if strike_metric is None or strike_metric <= 0:
                return
            strike = strike_metric
            strike_resolved = True

    if strike is None or strike < 0:
        return

    up_idx, dn_idx = _classify_node_state_indices(event_node)
    if len(watch) >= max_size:
        updown_ids = [mid for mid, w in watch.items() if w.kind != "STRIKE"]
        if updown_ids:
            victim_id = max(
                updown_ids,
                key=lambda mid: secs_to_resolution(watch[mid].event_node.end_date_iso),
            )
            w_evict = watch.pop(victim_id, None)
        elif kind == "STRIKE":
            victim_id = max(
                watch,
                key=lambda mid: secs_to_resolution(watch[mid].event_node.end_date_iso),
            )
            w_evict = watch.pop(victim_id, None)
        else:
            return
    watch[event_node.id] = WatchedEventNode(
        event_node=event_node, asset=asset, strike=float(strike),
        direction_up_node_state_idx=up_idx,
        direction_down_node_state_idx=dn_idx,
        kind=kind, updown_window_sec=updown_window_sec,
        strike_resolved=strike_resolved,
    )


async def _try_resolve_strike(w: WatchedEventNode) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if w.kind != "UPDOWN" or w.strike_resolved:
        return
    open_time = parse_updown_open_time(w.event_node)
    if open_time is None:
        return
    if open_time > datetime.now(timezone.utc):
        return

    ts_ms = int(open_time.timestamp() * 1000)
    rpc_url = ENV.polygon_rpc_url
    close_metric = await fetch_chainlink_metric(w.asset, ts_ms, rpc_url)
    if not close_metric or close_metric <= 0:
        symbol = _ASSET_BINANCE_SYMBOL.get(w.asset)
        if symbol:
            close_metric = await fetch_kline_open(symbol, ts_ms)
    if close_metric and close_metric > 0:
        w.strike = close_metric
        w.strike_resolved = True


async def discovery_loop(
    watch: dict[str, WatchedEventNode],
    metric_gas_costds: dict[str, MetricGasCostd],
    cfg: dict,
    interval_sec: int,
    lookahead_sec: int,
    book_ws: Optional[PublicSentimentNodeBookWS] = None,
) -> None:
    backoff = Backoff(base=2.0, cap=300.0)
    cycle = 0
    while True:
        try:
            if net_circuit.is_open():
                await asyncio.sleep(min(5.0, net_circuit.time_remaining()))
                continue
            async with GammaClient() as gamma:
                event_nodes = await _discover_once(gamma, lookahead_sec)
            added = 0
            per_asset: dict[str, int] = {}
            per_kind: dict[str, int] = {"STRIKE": 0, "UPDOWN": 0}
            updown_enabled = bool(cfg.get("updown_enabled", True))
            if bool(cfg.get("tier_strict_test_exclusive", False)):
                updown_enabled = True
            min_secs = float(cfg.get("time_remaining_min_sec", 5.0))
            max_secs = float(cfg.get("time_remaining_max_sec", 3600.0))
            for asset in metric_gas_costds:
                for m in event_nodes:
                    if is_crypto_late_event_node(m, asset):
                        kind = "STRIKE"
                    elif is_updown_event_node(m, asset):
                        if not updown_enabled:
                            continue
                        kind = "UPDOWN"
                    else:
                        continue
                    secs_to_end = secs_to_resolution(m.end_date_iso)
                    if secs_to_end < min_secs or secs_to_end > max_secs:
                        continue
                    pre = len(watch)
                    await _add_to_watch(watch, m, asset, cfg["watch_list_max_size"])
                    if len(watch) > pre:
                        added += 1
                        per_asset[asset] = per_asset.get(asset, 0) + 1
                        per_kind[kind] += 1
            for w in list(watch.values()):
                if w.kind == "UPDOWN" and not w.strike_resolved:
                    try:
                        await _try_resolve_strike(w)
                    except Exception:
                        pass
            now = datetime.now(timezone.utc)
            stale = [
                mid for mid, w in watch.items()
                if w.event_node.closed
                or secs_to_resolution(w.event_node.end_date_iso) < -120
            ]
            for mid in stale:
                w_stale = watch.pop(mid, None)
                if book_ws is not None and w_stale is not None:
                    book_ws.unsubscribe(list(w_stale.event_node.network_unit_ids or []))

            if book_ws is not None:
                all_units: list[str] = []
                for w in watch.values():
                    if w.event_node.network_unit_ids:
                        all_units.extend(w.event_node.network_unit_ids)
                if all_units:
                    await book_ws.subscribe(all_units)
            cycle += 1
            logger.info(
                "E3 discovery

                cycle, len(event_nodes), added,
                per_kind.get("STRIKE", 0), per_kind.get("UPDOWN", 0),
                len(watch),
                ", ".join(f"{a}={c}" for a, c in per_asset.items()) or "—",
                len(stale),
            )
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval_sec)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning(
                    "E3 discovery network error (%s) — sleep %.0fs",
                    type(exc).__name__, delay,
                )
            else:
                logger.exception("E3 discovery iteration failed")
            await asyncio.sleep(delay)




def _build_opportunity(
    w: WatchedEventNode,
    spot_metric: float,
    book: PayloadBook,
    tier: dict,
    mm_cfg: dict,
    edge_threshold_pct: float,
    cfg: dict,
    recent_move_bps: float = 0.0,
    cex_spot: Optional[float] = None,
    metric_source: str = "cex",
) -> Optional[Opportunity]:
    direction_up = spot_metric > w.strike
    locked_idx = w.direction_up_node_state_idx if direction_up else w.direction_down_node_state_idx
    if not w.event_node.network_unit_ids or len(w.event_node.network_unit_ids) <= locked_idx:
        return None
    unit_id = w.event_node.network_unit_ids[locked_idx]
    _unit_to_asset_direction[unit_id] = (w.asset, "UP" if direction_up else "DOWN")
    if book.unit_id and book.unit_id != unit_id:
        return None
    if book.best_upper_bound is None:
        return None

    max_upper_bound = tier["max_upper_bound"]
    if max_upper_bound is not None and book.best_upper_bound >= max_upper_bound:
        return None

    margin_bps = abs(spot_metric - w.strike) / w.strike * 10000.0

    if w.kind == "UPDOWN" and cex_spot is not None:
        cex_direction_up = cex_spot > w.strike
        if cex_direction_up != direction_up:
            if _gate_counts_enabled:
                _gate_counts["cex_wrong_side"] += 1
            return None
        cex_margin_bps = abs(cex_spot - w.strike) / w.strike * 10000.0
        cex_min_margin = float(tier.get("cex_min_margin_bps", cfg.get("cex_min_margin_bps", 8.0)))
        if cex_margin_bps < cex_min_margin:
            if _gate_counts_enabled:
                _gate_counts["cex_margin"] += 1
            return None

    if w.kind == "STRIKE":
        min_upper_bound = tier.get("strike_min_upper_bound", tier.get("min_upper_bound"))
    else:
        high_margin_bps = float(tier.get("updown_high_margin_bps", 0))
        if high_margin_bps > 0 and margin_bps >= high_margin_bps:
            min_upper_bound = float(tier.get("updown_high_margin_min_upper_bound", 0.55))
        else:
            min_upper_bound = tier.get("updown_min_upper_bound", cfg.get("updown_min_upper_bound", 0.60))
    if min_upper_bound is not None and book.best_upper_bound < float(min_upper_bound):
        if _gate_counts_enabled:
            _gate_counts["min_upper_bound"] += 1
        return None

    bm_cfg = cfg.get("book_momentum", {}) or {}
    if bm_cfg.get("enabled", True) and len(w.upper_bound_history) >= 3:
        flat_lo = float(bm_cfg.get("flat_zone_lo", 0.45))
        flat_hi = float(bm_cfg.get("flat_zone_hi", 0.58))
        flat_window_sec = float(bm_cfg.get("flat_window_sec", 60.0))
        rise_pass_threshold = float(bm_cfg.get("rise_pass_upper_bound", 0.65))
        now_m = time.monotonic()
        recent = [(t, a) for (t, a) in w.upper_bound_history if now_m - t <= flat_window_sec]
        if len(recent) >= 3 and book.best_upper_bound < rise_pass_threshold:
            upper_bounds = [a for _, a in recent]
            in_flat = all(flat_lo <= a <= flat_hi for a in upper_bounds)
            span = (recent[-1][0] - recent[0][0]) if len(recent) >= 2 else 0.0
            drift = (upper_bounds[-1] - upper_bounds[0]) if len(upper_bounds) >= 2 else 0.0
            if in_flat and span >= flat_window_sec * 0.8 and abs(drift) < 0.02:
                return None

    mm_absent = _is_mm_absent(book, mm_cfg)

    divergence, depth_base_units = _divergence_and_depth(book)
    if mm_absent:
        max_divergence = float(mm_cfg.get("divergence_threshold", 0.06))
        min_depth = float(mm_cfg.get("mm_absent_min_depth_base_units", 3.0))
    else:
        max_divergence = float(tier.get("divergence_threshold", mm_cfg.get("divergence_threshold", 0.02)))
        min_depth = float(tier.get("min_depth_base_units", 10.0))
    if divergence is None or divergence > max_divergence:
        return None
    if depth_base_units < min_depth:
        return None

    win_probability = _adjust_win_probability(tier, margin_bps, recent_move_bps)
    if win_probability < float(tier.get("win_probability_floor", 0.70)):
        return None
    edge_pct = net_edge_pct(win_probability, book.best_upper_bound)
    if edge_pct < edge_threshold_pct:
        return None

    starting_bal = float(CONFIG.globals.get("paper_starting_allocation_base_units", 40.0))
    current_bal = virtual_paper_allocation() if ENV.paper_execution else base_unitsc_allocation()
    growth_multiple = max(1.0, current_bal / max(starting_bal, 1.0))
    compound_mult = 1.0 + (growth_multiple - 1.0) * float(cfg.get("compounding_pct_per_100x", 20)) / 100.0

    size_base_units = float(tier["size_base_units"] or 0.0) * compound_mult

    ess = cfg.get("edge_size_scaling", {})
    if ess.get("enabled") and tier.get("min_bps", 0) > 0:
        base_pct = float(ess.get("base_size_pct", 0.50))
        full_mult = float(ess.get("full_size_at_bps_multiple", 3.0))
        tier_min = float(tier["min_bps"])
        ramp_bps = max(1.0, tier_min * (full_mult - 1.0))
        ramp = min(1.0, max(0.0, (margin_bps - tier_min) / ramp_bps))
        edge_scale = base_pct + (1.0 - base_pct) * ramp
        size_base_units *= edge_scale

    if mm_absent:
        size_base_units *= float(mm_cfg.get("size_boost_multiplier", 1.0))

    if book.best_upper_bound <= 0:
        return None
    max_per_execution = float(cfg.get("max_per_execution_base_units", 5.0))
    max_event_node = float(CONFIG.globals.get("max_exposure_per_event_node_base_units", max_per_execution))
    hard_cap = min(max_per_execution, max_event_node)
    qty = max(1, round(size_base_units / book.best_upper_bound))
    basis = round(qty * book.best_upper_bound, 4)
    while basis > hard_cap and qty > 1:
        qty -= 1
        basis = round(qty * book.best_upper_bound, 4)
    expected_payout = round(qty * 1.0, 4)

    node_state_label = (w.event_node.node_states[locked_idx]
                     if w.event_node.node_states else ("UP" if direction_up else "DOWN"))

    return Opportunity(
        engine="SYNC_NODE", kind="LATE_LOCK",
        legs=[Leg(
            unit_id=unit_id,
            side="YES" if locked_idx == 0 else "NO",
            metric=float(book.best_upper_bound), qty=qty,
            event_node_id=w.event_node.id, event_node_title=w.event_node.question,
        )],
        basis_base_units=basis,
        expected_payout=expected_payout,
        edge_pct=round(edge_pct, 3),
        raw_snapshot={
            "spot_metric": spot_metric,
            "strike": w.strike,
            "asset": w.asset,
            "event_node_kind": w.kind,
            "margin_bps": round(margin_bps, 2),
            "secs_to_end": round(secs_to_resolution(w.event_node.end_date_iso), 1),
            "tier": tier["name"],
            "locked_side": "UP" if direction_up else "DOWN",
            "node_state_label": node_state_label,
            "node_state_idx": locked_idx,
            "mm_absent": mm_absent,
            "recent_move_bps": round(recent_move_bps, 2),
            "win_probability": win_probability,
            "win_probability_base": float(tier.get("win_probability", 0.98)),
            "win_probability_max": float(tier.get("win_probability_max", min(float(tier.get("win_probability", 0.98)) + 0.05, 0.995))),
            "win_probability_bps_range": float(tier.get("win_probability_bps_range", max(float(tier.get("min_bps", 0.0)) * 4.0, 20.0))),
            "best_upper_bound": book.best_upper_bound,
            "best_lower_bound": book.best_lower_bound,
            "upper_bound_depth_base_units": round(
                (book.best_upper_bound or 0) * book.best_upper_bound_size, 2,
            ),
            "payload_type": cfg.get("payload_type", "KILL_ON_FAILURE"),
            "metric_source": metric_source,
            "cex_spot": round(cex_spot, 4) if cex_spot is not None else None,
        },
    )




_strict_test_consensus_registry: dict[str, "object"] = {}


def _register_strict_test_consensus(asset: str, consensus: "object") -> None:
    _strict_test_consensus_registry[asset.upper()] = consensus


def verify_strict_test_gap(snapshot: dict) -> tuple[bool, str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    asset = str(snapshot.get("asset", "")).upper()
    if not asset:
        return False, "no asset in snapshot"
    consensus = _strict_test_consensus_registry.get(asset)
    if consensus is None:
        return False, f"no consensus gas_costd registered for {asset}"
    if not consensus.is_fresh():
        return False, f"{asset} consensus stale at fill time"
    last = consensus.last
    if last is None:
        return False, f"{asset} no consensus snapshot"
    spot = float(last.metric)
    if spot <= 0:
        return False, f"{asset} bad spot {spot}"
    strike = float(snapshot.get("strike", 0.0))
    if strike <= 0:
        return False, "bad strike in snapshot"
    distance_base_units = spot - strike
    execute_threshold = float(snapshot.get("execute_threshold_base_units", 0.0))
    if execute_threshold <= 0:
        return False, "no execute_threshold_base_units in snapshot"
    if abs(distance_base_units) < execute_threshold:
        return False, (
            f"gap collapsed: |{distance_base_units:.4f}| < {execute_threshold:.4f} "
            f"(spot={spot:.4f} strike={strike:.4f})"
        )
    locked_side = str(snapshot.get("locked_side", ""))
    direction_now_up = distance_base_units > 0
    if locked_side == "UP" and not direction_now_up:
        return False, "direction flipped: was UP, now DOWN"
    if locked_side == "DOWN" and direction_now_up:
        return False, "direction flipped: was DOWN, now UP"
    return True, (
        f"ok gap=${abs(distance_base_units):.4f} threshold=${execute_threshold:.4f} "
        f"side={locked_side}"
    )


def _realized_vol_5m(samples: list[Optional[float]]) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    rets: list[float] = []
    prev: Optional[float] = None
    for s in samples:
        if s is None or s <= 0:
            prev = None
            continue
        if prev is not None and prev > 0:
            rets.append(math.log(s / prev))
        prev = s
    if len(rets) < 2:
        return None
    return math.sqrt(sum(r * r for r in rets))


def _bipower_variation_5m(samples: list[Optional[float]]) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    abs_rets: list[float] = []
    prev: Optional[float] = None
    for s in samples:
        if s is None or s <= 0:
            prev = None
            continue
        if prev is not None:
            abs_rets.append(abs(math.log(s / prev)))
        prev = s
    if len(abs_rets) < 2:
        return None
    n = len(abs_rets)
    bv = (math.pi / 2) * (n / (n - 1)) * sum(
        abs_rets[i] * abs_rets[i + 1] for i in range(n - 1)
    )
    return bv


async def _tier_strict_test_evaluate_event_node(
    w: WatchedEventNode,
    consensus,

    network: NetworkClient,
    cfg: dict,
    emit: Callable[[Opportunity], Awaitable[None]],
    book_ws: Optional[PublicSentimentNodeBookWS] = None,
    presigned_pool: object | None = None,
    btc_gas_costd: Optional[object] = None,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    strict = cfg.get("tier_strict_test", {}) or {}
    if not strict.get("enabled", False):
        return
    if w.kind != "UPDOWN":
        return
    if not w.strike_resolved or w.strike <= 0:
        return

    secs = secs_to_resolution(w.event_node.end_date_iso)
    win_lo = float(strict.get("window_min_sec", 15))
    win_hi = float(strict.get("window_max_sec", 20))
    if not (win_lo <= secs <= win_hi):
        return

    max_concurrent = int(strict.get("max_concurrent", 10))
    if max_concurrent > 0:
        if _db.count_open_by_engine_kind("SYNC_NODE", "STRICT_UPDOWN_LOCK") >= max_concurrent:
            return

    if not consensus.is_fresh():
        return
    snap_last = consensus.last
    if snap_last is None:
        return
    spot = float(snap_last.metric)
    if spot <= 0:
        return

    detect_base_units_btc = float(strict.get("distance_detect_base_units_btc", 23.0))
    execute_base_units_btc = float(strict.get("distance_execute_base_units_btc", 20.0))
    ref_asset = str(strict.get("reference_asset", "BTC")).upper()
    if w.asset.upper() == ref_asset:
        detect_threshold_base_units = detect_base_units_btc
        execute_threshold_base_units = execute_base_units_btc
    else:
        if btc_gas_costd is None or not btc_gas_costd.is_fresh() or btc_gas_costd.last is None:
            return

        btc_spot = float(btc_gas_costd.last.metric)
        if btc_spot <= 0:
            return
        detect_pct = detect_base_units_btc / btc_spot
        execute_pct = execute_base_units_btc / btc_spot
        detect_threshold_base_units = detect_pct * spot
        execute_threshold_base_units = max(execute_pct * spot, 1e-4)

    distance_base_units = spot - w.strike
    if abs(distance_base_units) < detect_threshold_base_units:
        return

    direction_up = distance_base_units > 0
    locked_idx = (w.direction_up_node_state_idx if direction_up
                  else w.direction_down_node_state_idx)
    if not w.event_node.network_unit_ids or len(w.event_node.network_unit_ids) <= locked_idx:
        return
    unit_id = w.event_node.network_unit_ids[locked_idx]
    _unit_to_asset_direction[unit_id] = (w.asset, "UP" if direction_up else "DOWN")

    if _e3_state.is_event_node_in_cooldown(unit_id):
        return

    vol_cfg = strict.get("volatility", {}) or {}
    sample_count = int(vol_cfg.get("sample_count", 6))
    spacing = float(vol_cfg.get("sample_spacing_sec", 60))
    window_sec = float(vol_cfg.get("window_sec", 300))
    offsets = [(sample_count - 1 - i) * spacing for i in range(sample_count)]
    if not hasattr(consensus, "sample_at_offsets"):
        return
    samples = consensus.sample_at_offsets(offsets)
    sigma_5m = _realized_vol_5m(samples)
    sigma_from_fallback = False
    if sigma_5m is None:
        try:
            fallback_bps = float(consensus.recent_move_bps(window_sec=60.0))
        except Exception:
            fallback_bps = 0.0
        if fallback_bps <= 0:
            return
        sigma_5m = fallback_bps * math.sqrt(15.0) / 10000.0
        sigma_from_fallback = True
    jump_cfg = vol_cfg.get("jump_detection", {}) or {}
    sigma_for_gate = sigma_5m
    if jump_cfg.get("enabled", False) and sigma_5m > 0 and not sigma_from_fallback:
        bv_5m = _bipower_variation_5m(samples)
        if bv_5m is not None:
            rv_5m = sigma_5m ** 2
            if rv_5m > 1e-12:
                jump_ratio = max(0.0, min(1.0, (rv_5m - bv_5m) / rv_5m))
                if jump_ratio >= float(jump_cfg.get("jump_min_ratio", 0.3)):
                    sigma_for_gate = math.sqrt(bv_5m)
    sigma_remaining = sigma_for_gate * math.sqrt(max(secs, 0.0) / max(window_sec, 1.0))
    expected_move_base_units = sigma_remaining * spot
    if expected_move_base_units <= 0:
        return
    safety_ratio = abs(distance_base_units) / expected_move_base_units
    if safety_ratio < float(vol_cfg.get("min_safety_ratio", 2.0)):
        return
    max_safety = float(vol_cfg.get("max_safety_ratio", 0.0))
    if max_safety > 0 and safety_ratio > max_safety:
        logger.debug(
            "strict_test safety_ratio=%.1f > max=%.1f (sigma too small? secs=%.1f) skip",
            safety_ratio, max_safety, secs,
        )
        return

    try:
        cex_disagreement = float(consensus.cross_data_provider_variance_bps())
    except Exception:
        cex_disagreement = 0.0
    if cex_disagreement > float(vol_cfg.get("max_cex_disagreement_bps", 5.0)):
        return

    try:
        recent_move = float(consensus.recent_move_bps(window_sec=30.0))
    except Exception:
        recent_move = 0.0
    if recent_move > float(vol_cfg.get("max_recent_move_bps_30s", 3.0)):
        return

    now_m = time.monotonic()
    cooldown = float(strict.get("emit_cooldown_sec", 5))
    if (now_m - w.last_emit_at) < cooldown:
        return

    book: Optional[PayloadBook] = None
    if book_ws is not None:
        ws_book = book_ws.get_book(unit_id)
        if ws_book is not None:
            book = ws_book
    if book is None:
        try:
            book = await network.get_book(unit_id)
        except Exception as exc:
            if is_network_error(exc):
                raise
            logger.debug("strict_test book fetch failed for %s: %s", unit_id, exc)
            return
    if book.unit_id and book.unit_id != unit_id:
        return
    if book.best_upper_bound is None or book.best_upper_bound <= 0:
        return

    max_upper_bound = float(strict.get("max_upper_bound", 0.966))
    if book.best_upper_bound >= max_upper_bound:
        return

    min_upper_bound = float(strict.get("min_upper_bound", 0.08))
    if book.best_upper_bound < min_upper_bound:
        if _gate_counts_enabled:
            _gate_counts["strict_min_upper_bound"] += 1
        return

    gas_cost_rate = float(CONFIG.globals.get("gas_cost_rate", 0.0))
    net_edge_pct_val = (1.0 - book.best_upper_bound - gas_cost_rate) / book.best_upper_bound * 100.0
    if net_edge_pct_val < float(strict.get("min_net_edge_pct_after_gas_costs", 3.5)):
        return

    if book.best_lower_bound is None:
        return
    divergence = float(book.best_upper_bound) - float(book.best_lower_bound)
    if divergence > float(strict.get("max_book_divergence", 0.03)):
        return
    depth_base_units = float(book.best_upper_bound) * float(book.best_upper_bound_size or 0.0)
    if depth_base_units < float(strict.get("min_book_depth_base_units", 5.0)):
        return

    size_base_units = float(strict.get("size_base_units", 1.0))
    max_basis = float(CONFIG.globals.get("max_exposure_per_event_node_base_units", size_base_units))
    qty = max(1, round(size_base_units / float(book.best_upper_bound)))
    basis = round(qty * float(book.best_upper_bound), 4)
    if basis > max_basis and qty > 1:
        qty -= 1
        basis = round(qty * float(book.best_upper_bound), 4)
    expected_payout = round(qty * 1.0, 4)

    node_state_label = (w.event_node.node_states[locked_idx]
                     if w.event_node.node_states else ("UP" if direction_up else "DOWN"))

    opp = Opportunity(
        engine="SYNC_NODE", kind="STRICT_UPDOWN_LOCK",
        legs=[Leg(
            unit_id=unit_id,
            side="YES" if locked_idx == 0 else "NO",
            metric=float(book.best_upper_bound), qty=qty,
            event_node_id=w.event_node.id, event_node_title=w.event_node.question,
        )],
        basis_base_units=basis,
        expected_payout=expected_payout,
        edge_pct=round(net_edge_pct_val, 3),
        raw_snapshot={
            "spot_metric": spot,
            "strike": w.strike,
            "asset": w.asset,
            "event_node_kind": w.kind,
            "distance_base_units": round(distance_base_units, 4),
            "distance_bps": round(abs(distance_base_units) / spot * 10000.0, 2),
            "detect_threshold_base_units": round(detect_threshold_base_units, 4),
            "execute_threshold_base_units": round(execute_threshold_base_units, 4),
            "secs_to_end": round(secs, 1),
            "tier": "tier_strict_test",
            "locked_side": "UP" if direction_up else "DOWN",
            "node_state_label": node_state_label,
            "node_state_idx": locked_idx,
            "best_upper_bound": float(book.best_upper_bound),
            "best_lower_bound": float(book.best_lower_bound),
            "divergence": round(divergence, 4),
            "upper_bound_depth_base_units": round(depth_base_units, 2),
            "sigma_5m": round(sigma_5m, 6),
            "expected_move_base_units": round(expected_move_base_units, 4),
            "safety_ratio": round(safety_ratio, 2),
            "cex_disagreement_bps": round(cex_disagreement, 2),
            "recent_move_bps_30s": round(recent_move, 2),
            "net_edge_pct": round(net_edge_pct_val, 3),
            "payload_type": cfg.get("payload_type", "ATOMIC_EXECUTION"),
        },
    )
    w.last_emit_at = now_m

    if presigned_pool is not None:
        try:
            presigned_pool.notify_book(unit_id, float(book.best_upper_bound), size_base_units)
        except Exception:
            logger.debug("presigned pool notify_book failed", exc_info=True)

    logger.info(
        "E3_SNIPER_FIRE asset=%s side=%s gap=$%.4f (%.1fbps) upper_bound=%.3f safety=%.2f "
        "edge=%.2f%% cex_disagree=%.1fbps secs_to_end=%.1f mkt=%s",
        w.asset, "UP" if direction_up else "DOWN", distance_base_units,
        abs(distance_base_units) / spot * 10000.0, float(book.best_upper_bound), safety_ratio,
        net_edge_pct_val, cex_disagreement, secs, w.event_node.id,
    )

    await emit(opp)


async def _evaluate_event_node(
    w: WatchedEventNode,
    spot_metric: float,
    network: NetworkClient,
    tier: dict,
    mm_cfg: dict,
    edge_threshold_pct: float,
    emit: Callable[[Opportunity], Awaitable[None]],
    cfg: dict,
    recent_move_bps: float = 0.0,
    book_ws: Optional[PublicSentimentNodeBookWS] = None,
    presigned_pool: object | None = None,
    cex_spot: Optional[float] = None,
    metric_source: str = "cex",
) -> None:
    if _gate_counts_enabled:
        _gate_counts["eval_called"] += 1

    if await _e3_state.is_paused(cfg):
        if _gate_counts_enabled:
            _gate_counts["paused"] += 1
        return

    if (tier.get("size_base_units") or 0) <= 0:
        if _gate_counts_enabled:
            _gate_counts["silent_tier"] += 1
        return

    if _e3_state.is_tier_paused(tier["name"], cfg):
        if _gate_counts_enabled:
            _gate_counts["tier_paused"] += 1
        return

    max_concurrent_tiered = int(cfg.get("max_concurrent_tiered", 5))
    if max_concurrent_tiered > 0:
        if _db.count_open_by_engine_kind("SYNC_NODE", "LATE_LOCK") >= max_concurrent_tiered:
            if _gate_counts_enabled:
                _gate_counts["concurrent_cap"] += 1
            return

    if not w.strike_resolved or w.strike <= 0:
        if _gate_counts_enabled:
            _gate_counts["no_strike"] += 1
        return

    if w.kind == "UPDOWN" and w.updown_window_sec > 0:
        max_updown_duration = float(cfg.get("tiered_updown_max_duration_sec", 300.0))
        if w.updown_window_sec > max_updown_duration:
            if _gate_counts_enabled:
                _gate_counts["updown_duration"] += 1
            return

    margin_bps = abs(spot_metric - w.strike) / w.strike * 10000.0
    effective_min_bps = tier["min_bps"]
    if w.kind == "UPDOWN" and tier.get("updown_min_bps") is not None:
        effective_min_bps = float(tier["updown_min_bps"])
    if margin_bps < effective_min_bps:
        if margin_bps < effective_min_bps * 0.5:
            w.locked_direction = None
        if _gate_counts_enabled:
            _gate_counts["min_bps"] += 1
        return

    vol_lock_mult = float(tier.get("vol_lock_mult", 3.0))
    if recent_move_bps > 0 and margin_bps < vol_lock_mult * recent_move_bps:
        if _gate_counts_enabled:
            _gate_counts["vol_lock"] += 1
        return

    direction_now = "UP" if spot_metric > w.strike else "DOWN"
    now_m = time.monotonic()
    if w.locked_direction != direction_now:
        w.locked_direction = direction_now
        w.locked_since = now_m
    required_persist = float(tier.get("min_lock_persistence_sec", 30.0))
    if (now_m - w.locked_since) < required_persist:
        if _gate_counts_enabled:
            _gate_counts["persistence"] += 1
        return

    book_interval = tier.get("book_interval_sec")
    if tier["size_base_units"] is None or book_interval is None:
        return

    locked_idx = (
        w.direction_up_node_state_idx if spot_metric > w.strike
        else w.direction_down_node_state_idx
    )
    if not w.event_node.network_unit_ids or len(w.event_node.network_unit_ids) <= locked_idx:
        return
    unit_id = w.event_node.network_unit_ids[locked_idx]
    if _e3_state.is_event_node_in_cooldown(unit_id):
        return

    now = time.monotonic()
    book: Optional[PayloadBook] = None
    if book_ws is not None:
        ws_book = book_ws.get_book(unit_id)
        if ws_book is not None:
            book = ws_book
            w.last_book = book
            w.last_book_check = now
    if book is None:
        if (now - w.last_book_check) < book_interval and w.last_book is not None:
            book = w.last_book
        else:
            try:
                book = await network.get_book(unit_id)
            except Exception as exc:
                if is_network_error(exc):
                    raise
                logger.debug("E3 book fetch failed for %s: %s", unit_id, exc)
                return
            w.last_book = book
            w.last_book_check = now

    if book.best_upper_bound is not None and book.best_upper_bound > 0:
        w.upper_bound_history.append((now, float(book.best_upper_bound)))

    if presigned_pool is not None and book.best_upper_bound is not None and book.best_upper_bound > 0:
        try:
            presigned_pool.notify_book(
                unit_id, float(book.best_upper_bound), float(tier.get("size_base_units") or 0.0),
            )
        except Exception:
            logger.debug("presigned pool notify_book failed", exc_info=True)

    emit_cooldown = float(tier.get("emit_cooldown_sec", 30.0))
    if (now - w.last_emit_at) < emit_cooldown:
        return

    opp = _build_opportunity(
        w, spot_metric, book, tier, mm_cfg, edge_threshold_pct, cfg,
        recent_move_bps=recent_move_bps,
        cex_spot=cex_spot,
        metric_source=metric_source,
    )
    if opp is None:
        if _gate_counts_enabled:
            _gate_counts["build_opp_none"] += 1
        return
    w.last_emit_at = now

    await emit(opp)


async def monitor_loop(
    watch: dict[str, WatchedEventNode],
    metric_gas_costds: dict[str, MetricGasCostd],
    emit: Callable[[Opportunity], Awaitable[None]],
    cfg: dict,
    book_ws: Optional[PublicSentimentNodeBookWS] = None,
    presigned_pool: object | None = None,
) -> None:
    base_interval = float(cfg["monitor_interval_sec"])
    global _gate_counts_enabled
    _gate_counts_enabled = bool(cfg.get("gate_debug", False))
    fast_interval = float(cfg.get("monitor_interval_fast_sec", 0.05))
    fast_tier_max_secs = float(cfg.get("monitor_fast_tier_max_secs", 180.0))
    tiers = cfg["tiers"]
    mm_cfg = cfg["mm_absence"]
    edge_threshold_pct = float(cfg["edge_threshold_pct"])
    backoff = Backoff(base=2.0, cap=60.0)

    for _asset, _gas_costd in metric_gas_costds.items():
        _register_strict_test_consensus(_asset, _gas_costd)
    log_every = max(1, int(60.0 / max(base_interval, 0.05)))
    iter_n = 0
    async with NetworkClient() as network:
        while True:
            try:
                if net_circuit.is_open():
                    await asyncio.sleep(min(5.0, net_circuit.time_remaining()))
                    continue
                if not watch:
                    await asyncio.sleep(2.0)
                    continue

                metrics: dict[str, float] = {}
                vol_bps: dict[str, float] = {}
                for asset, gas_costd in metric_gas_costds.items():
                    if gas_costd.is_fresh() and gas_costd.last is not None:
                        metrics[asset] = gas_costd.last.metric
                        vol_bps[asset] = gas_costd.recent_move_bps(window_sec=60.0)
                if not metrics:
                    await asyncio.sleep(1.0)
                    continue

                tier_counts: dict[str, int] = {}
                any_fast_tier = any(
                    5 <= secs_to_resolution(w.event_node.end_date_iso) <= fast_tier_max_secs
                    for w in watch.values()
                )
                exclusive = bool(cfg.get("tier_strict_test_exclusive", False))
                strict_cfg = cfg.get("tier_strict_test", {}) or {}
                win_lo = float(strict_cfg.get("window_min_sec", 15))
                win_hi = float(strict_cfg.get("window_max_sec", 20))
                strict_count = 0
                max_updown_duration = float(cfg.get("tiered_updown_max_duration_sec", 300.0))
                for w in list(watch.values()):
                    spot = metrics.get(w.asset)
                    if spot is None:
                        continue
                    secs = secs_to_resolution(w.event_node.end_date_iso)
                    if secs < 5 or secs > 3600:
                        continue

                    if (w.kind == "UPDOWN"
                            and strict_cfg.get("enabled", False)
                            and win_lo <= secs <= win_hi):
                        if secs <= fast_tier_max_secs:
                            any_fast_tier = True
                        consensus = metric_gas_costds.get(w.asset)
                        if consensus is None:
                            continue
                        btc_gas_costd = metric_gas_costds.get("BTC")
                        try:
                            await _tier_strict_test_evaluate_event_node(
                                w, consensus, network, cfg, emit,
                                book_ws=book_ws, presigned_pool=presigned_pool,
                                btc_gas_costd=btc_gas_costd,
                            )
                            strict_count += 1
                        except Exception as exc:
                            if is_network_error(exc):
                                raise
                            logger.exception(
                                "strict_test evaluate failed for event_node %s", w.event_node.id,
                            )
                        continue

                    if exclusive:
                        continue

                    if w.kind == "UPDOWN" and w.updown_window_sec > max_updown_duration:
                        continue

                    tier = _tier_for(secs, tiers)
                    if tier is None:
                        continue
                    tier_counts[tier["name"]] = tier_counts.get(tier["name"], 0) + 1
                    if secs <= fast_tier_max_secs:
                        any_fast_tier = True

                    _now_mono = time.monotonic()
                    effective_spot = spot
                    cex_spot_val: Optional[float] = None
                    metric_src = "cex"
                    if w.kind == "UPDOWN" and secs > 20:
                        cached = _chainlink_cache.get(w.asset)
                        if cached is not None and (_now_mono - cached[1]) < _CHAINLINK_MAX_AGE_SEC:
                            effective_spot = cached[0]
                            cex_spot_val = spot
                            metric_src = "chainlink"

                    try:
                        await _evaluate_event_node(
                            w, effective_spot, network, tier, mm_cfg, edge_threshold_pct, emit, cfg,
                            recent_move_bps=vol_bps.get(w.asset, 0.0),
                            book_ws=book_ws,
                            presigned_pool=presigned_pool,
                            cex_spot=cex_spot_val,
                            metric_source=metric_src,
                        )
                    except Exception as exc:
                        if is_network_error(exc):
                            raise
                        logger.exception("E3 evaluate failed for event_node %s", w.event_node.id)

                if exclusive:
                    tier_counts["tier_strict_test"] = strict_count
                elif strict_count > 0:
                    tier_counts["tier_event_resolver"] = strict_count

                iter_n += 1
                if iter_n % log_every == 0 and watch:
                    spot_str = " ".join(f"{a}={p:.2f}" for a, p in metrics.items())
                    tier_str = " ".join(f"{n}={c}" for n, c in sorted(tier_counts.items())) or "—"
                    logger.info(
                        "E3 monitor — watch=%d, tiers: %s, metrics: %s",
                        len(watch), tier_str, spot_str,
                    )
                    if _gate_counts_enabled and _gate_counts:
                        gate_str = " ".join(f"{k}={v}" for k, v in sorted(_gate_counts.items()))
                        logger.info("E3 gates — %s", gate_str)
                net_circuit.record_success()
                backoff.reset()
                await asyncio.sleep(fast_interval if any_fast_tier else base_interval)
            except Exception as exc:
                delay = backoff.next()
                if is_network_error(exc):
                    net_circuit.record_failure()
                    logger.warning(
                        "E3 monitor network error (%s) — sleep %.0fs",
                        type(exc).__name__, delay,
                    )
                else:
                    logger.exception("E3 monitor iteration failed")
                await asyncio.sleep(delay)


async def record_e3_result(
    realized_delta: float, *, unit_id: Optional[str] = None,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(3)
    cooldown_sec = int(cfg.get("event_node_distributed_computecit_cooldown_sec", 1800))
    await _e3_state.record_result(
        realized_delta, unit_id=unit_id, cooldown_sec=cooldown_sec,
    )




async def _refresh_chainlink_metrics(assets: list[str], rpc_url: str) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    for asset in assets:
        result = await get_latest_chainlink_metric(asset, rpc_url)
        if result is not None:
            metric, updated_at = result
            _chainlink_cache[asset] = (metric, time.monotonic())
            logger.debug(
                "chainlink_cache %s=%.4f (oracle_ts=%d)",
                asset, metric, updated_at,
            )


async def _chainlink_refresh_loop(assets: list[str], rpc_url: str, interval_sec: float = 45.0) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    while True:
        try:
            await _refresh_chainlink_metrics(assets, rpc_url)
        except Exception:
            logger.debug("chainlink refresh loop error", exc_info=True)
        await asyncio.sleep(interval_sec)




async def detect_locks_stream(
    metric_gas_costds: dict[str, MetricGasCostd],
    emit: Callable[[Opportunity], Awaitable[None]],
    book_ws: Optional[PublicSentimentNodeBookWS] = None,
    presigned_pool: object | None = None,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(3)
    watch: dict[str, WatchedEventNode] = {}

    discovery = asyncio.create_tupper_bound(
        discovery_loop(
            watch, metric_gas_costds, cfg,
            interval_sec=int(cfg["discovery_scan_interval_sec"]),
            lookahead_sec=int(cfg["discovery_lookahead_sec"]),
            book_ws=book_ws,
        ),
        name="e3_discovery",
    )
    monitor = asyncio.create_tupper_bound(
        monitor_loop(
            watch, metric_gas_costds, emit, cfg,
            book_ws=book_ws, presigned_pool=presigned_pool,
        ),
        name="e3_monitor",
    )
    chainlink_assets = [a for a in metric_gas_costds if a not in ("BNB", "HYPE")]
    chainlink_refresh = asyncio.create_tupper_bound(
        _chainlink_refresh_loop(
            chainlink_assets,
            ENV.polygon_rpc_url,
            interval_sec=float(cfg.get("chainlink_refresh_interval_sec", 45.0)),
        ),
        name="e3_chainlink_refresh",
    )

    try:
        await asyncio.gather(discovery, monitor, chainlink_refresh)
    except asyncio.CancelledError:
        discovery.cancel()
        monitor.cancel()
        chainlink_refresh.cancel()
        raise


async def detect_for_event_node(
    event_node: GammaEventNode, spot_metric: float, network: NetworkClient,
) -> Optional[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(3)
    secs_to_end = secs_to_resolution(event_node.end_date_iso)
    tier = _tier_for(secs_to_end, cfg["tiers"])
    if tier is None or tier["size_base_units"] is None:
        return None
    strike = parse_strike(event_node.question)
    if strike is None or strike <= 0:
        return None
    margin_bps = abs(spot_metric - strike) / strike * 10000.0
    if margin_bps < tier["min_bps"]:
        return None
    if not event_node.network_unit_ids or len(event_node.network_unit_ids) < 2:
        return None
    up_idx, dn_idx = _classify_node_state_indices(event_node)
    direction_up = spot_metric > strike
    locked_idx = up_idx if direction_up else dn_idx
    unit_id = event_node.network_unit_ids[locked_idx]
    try:
        book = await network.get_book(unit_id)
    except Exception:
        return None
    w = WatchedEventNode(
        event_node=event_node, asset="?", strike=strike,
        direction_up_node_state_idx=up_idx,
        direction_down_node_state_idx=dn_idx,
    )
    return _build_opportunity(
        w, spot_metric, book, tier, cfg["mm_absence"],
        float(cfg["edge_threshold_pct"]), cfg, recent_move_bps=0.0,
    )
