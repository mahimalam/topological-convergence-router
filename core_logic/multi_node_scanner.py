"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from .. import db
from ..common.gas_costs import net_edge_pct, FEE_MULT, classify_event_node_category, consumer_gas_cost_pct
from ..common import net_circuit
from ..common.net_errors import Backoff, is_network_error
from ..config import CONFIG, ENV
from ..ingestion.network_client import NetworkClient, PayloadBook
from ..ingestion.gamma_client import GammaClient, GammaEvent, GammaEventNode
from .opportunity import Leg, Opportunity

logger = logging.getLogger(__name__)



MIN_ACTIONABLE_ASK = 0.01
TICK = 0.01
SCAN_CONCURRENCY = 20
_STRUCTURAL_E1_KINDS = {"UNDER_SUM", "OVER_SUM", "BINARY_SUM", "ASYMMETRIC_DEPTH"}


_TOP_K_RE = re.compile(r'top\s+(\d+)', re.IGNORECASE)

_MEX_KEYWORDS = (
    "winner", "who will", "which one", "will win", "champion",
    "president", "primary winner", "nominee", "medalist", "championship",
    "election winner", "top goalscorer", "top scorer", "most seats",
)

_MULTI_KEYWORDS = (
    "relegat", "top ", "finish in the top", "qualify", "advance",
    "make the", "proceed", "survive", "stay in",
)

_SUBMARKET_TYPE_KEYWORDS = (
    "winner", "handicap", "divergence",
    "map ", "game ", "set ", "quarter ", "half ", "period ",
    "race ", "stage ", "round ", "leg ", "match ", "fight ",
)


def _event_nodes_fraction_question_stem(event_nodes: list[GammaEventNode]) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if len(event_nodes) < 2:
        return True
    questions = [m.question.strip() for m in event_nodes if m.question]
    if len(questions) < 2:
        return True
    prefix = _majority_common_prefix(questions)
    if not prefix:
        return False
    negative_vectorest = min(len(q) for q in questions)
    prefix_ratio = len(prefix) / negative_vectorest if negative_vectorest > 0 else 0
    if prefix_ratio < 0.4:
        return False
    suffixes = [q[len(prefix):].strip().strip("?:-. ") for q in questions]
    non_empty = [s for s in suffixes if s]
    if len(non_empty) < len(questions) * 0.5:
        return False
    combined_suffix = " ".join(non_empty).lower()
    for kw in _SUBMARKET_TYPE_KEYWORDS:
        if kw.lower() in combined_suffix:
            return False
    return True


def _majority_common_prefix(strings: list[str], min_agreement: float = 0.6) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not strings:
        return ""
    n = len(strings)
    min_count = max(2, int(n * min_agreement))
    ref = strings[0].lower()
    best = ""
    for i in range(len(ref)):
        ch = ref[i]
        count = sum(1 for s in strings if i < len(s) and s[i].lower() == ch)
        if count >= min_count:
            best = strings[0][: i + 1]
        else:
            break
    return best


def _common_prefix(strings: list[str]) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not strings:
        return ""
    ref = strings[0].lower()
    for i, ch in enumerate(ref):
        for s in strings[1:]:
            if i >= len(s) or s[i].lower() != ch:
                return strings[0][:i]
    return strings[0]


def detect_event_format(event: GammaEvent) -> tuple[str, int | None]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if event.neg_risk and _event_nodes_fraction_question_stem(event.event_nodes):
        return ("mutual_exclusive", 1)

    title_lower = (event.title or "").lower()
    desc_lower = (event.description or "").lower()
    combined = f"{title_lower} {desc_lower}"

    top_match = _TOP_K_RE.search(combined)
    if top_match:
        k = int(top_match.group(1))
        return ("top_k", k)

    if any(kw in combined for kw in _MEX_KEYWORDS):
        if _event_nodes_fraction_question_stem(event.event_nodes):
            return ("mutual_exclusive", 1)
        return ("independent", None)

    if any(kw in combined for kw in _MULTI_KEYWORDS):
        return ("independent", None)

    return ("unknown", None)


_FIELD_KEYWORDS = ("field", "other", "none of", "someone else", "any other")

_SLUG_PREFIX_LEN = 2


def _topic_key(slug: str) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not slug:
        return ""
    parts = slug.split("-")
    return "-".join(parts[:_SLUG_PREFIX_LEN])



def _hours_to(end_iso: Optional[str]) -> float:
    if not end_iso:
        return 9999.0
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return (end - datetime.now(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        return 9999.0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _topic_key(slug: str) -> str:
    if not slug:
        return ""
    parts = slug.split("-")
    return "-".join(parts[:_SLUG_PREFIX_LEN])


_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "sports": (
        "nba", "nfl", "mlb", "nhl", "mls", "nwsl", "epl", "uefa", "ucl",
        "champions-league", "champions league", "premier-league", "premier league",
        "la-liga", "la liga", "serie-a", "serie a", "bundesliga", "ligue",
        "eredivisie", "ere-", "k-league", "kor-", "j-league", "j1100",
        "bra-", "per1-", "bol1-", "chi1-", "den-", "scot",
        "vs-", "-vs-", "match", "game", "tournament", "playoff",
        "world-cup", "world cup", "olympics", "wimbledon",
        "open-tennis", "tennis", "golf", "f1-", "formula-1", "ufc",
        "boxing", "cricket", "rugby",
    ),
    "politics": (
        "election", "president", "primary", "nominee", "senate", "congress",
        "governor", "parliament", "pm-", "prime-minister", "prime minister",
        "vote", "ballot", "campaign", "polling", "trump", "lower_bounden",
        "harris", "vance", "putin", "zelensky",
    ),
    "news": (
        "ceasefire", "treaty", "summit", "war", "invade", "invasion",
        "sanction", "indictment", "resign", "indicted", "arrest",
        "supreme-court", "scotus", "fed-", "fed rate", "fomc",
        "rate-decision", "rate decision", "cpi", "inflation", "gdp",
        "unemployment", "nonfarm", "payrolls",
    ),
    "crypto_event": (
        "btc-", "bitcoin", "ether", "eth-", "sol-", "solana",
        "halving", "etf", "unit-listing", "airdrop", "fork",
        "hack", "exploit", "stablecoin", "depeg",
    ),
    "macro": (
        "recession", "gdp", "rate-cut", "rate cut", "rate-hike", "rate hike",
        "oil-metric", "oil metric", "opec", "yield-curve",
    ),
    "entertainment": (
        "oscar", "grammy", "emmy", "bafta", "tony-award",
        "box-office", "box office", "netflix", "disney",
        "billboard", "single", "album",
    ),
}


def _classify_topic(event: "GammaEvent") -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    haystack = " ".join([
        (event.title or "").lower(),
        (event.slug or "").lower(),
        (event.description or "").lower()[:200],
    ])
    for topic, keywords in _TOPIC_KEYWORDS.items():
        for kw in keywords:
            if kw in haystack:
                return topic
    return "default"


def _topic_weight(event: "GammaEvent") -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1).get("topic_weights") or {}
    topic = _classify_topic(event)
    try:
        return float(cfg.get(topic, cfg.get("default", 1.0)))
    except (TypeError, ValueError):
        return 1.0


def _is_field_node_state(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in _FIELD_KEYWORDS)


_NON_EXHAUSTIVE_PATTERNS = (
    "exact-score", "exact score", "correct-score", "correct score",
    "final-score", "final score", "exact-result", "exact result",
    "exact-time", "exact time", "exact-minute", "exact minute",
    "exact-margin", "exact margin", "winning-margin", "winning margin",
    "first-goal-time", "first goal time", "scoreline",
)


def _is_non_exhaustive_event(event: GammaEvent) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    haystack = " ".join([
        (event.title or "").lower(),
        (event.slug or "").lower(),
    ])
    return any(p in haystack for p in _NON_EXHAUSTIVE_PATTERNS)


def _event_field_event_nodes(event_nodes: list[GammaEventNode]) -> list[GammaEventNode]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    return [m for m in event_nodes if _is_field_node_state(m.question)]


_EVENT_TEMPLATE_WHITELIST = (
    "winner", "who-will-win", "will-win", "champion",
    "primary", "nominee", "election",
    "next-manager", "next-coach", "next-pm",
    "top-", "make-the-",
    "vs-", "-vs-",
    "will-",
)

_EVENT_TEMPLATE_BLACKLIST = (
    "exact-score", "correct-score", "final-score", "exact-result",
    "exact-time", "exact-minute", "exact-margin", "winning-margin",
    "first-goal-time", "scoreline", "total-corners",
    "total-cards", "btts",

)


def _passes_event_template_gate(event: GammaEvent) -> tuple[bool, str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1)
    if not cfg.get("event_template_gate", {}).get("enabled", True):
        return True, "gate_disabled"
    haystack = " ".join([
        (event.slug or "").lower(),
        (event.title or "").lower(),
    ])
    for bad in _EVENT_TEMPLATE_BLACKLIST:
        if bad in haystack:
            return False, f"blacklist:{bad}"
    if not cfg.get("event_template_gate", {}).get("require_whitelist", False):
        return True, "no_whitelist_required"
    for good in _EVENT_TEMPLATE_WHITELIST:
        if good in haystack:
            return True, f"whitelist:{good}"
    return False, "no_whitelist_match"


def _tier_for(state_depth: float) -> dict[str, Any]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1)
    tiers = cfg.get("state_depth_tiers") or []
    for tier in sorted(tiers, key=lambda t: float(t.get("min_state_depth_base_units", 0)), reverse=True):
        if state_depth >= float(tier.get("min_state_depth_base_units", 0)):
            return tier
    return {
        "name": "default",
        "edge_threshold_pct": float(cfg.get("edge_threshold_pct", 1.8)),
    }


def _min_economic_edge_pct() -> float:
    cfg = CONFIG.engine(1)
    if ENV.paper_execution:
        return float(cfg.get("min_economic_edge_paper_pct", 2.5))
    return float(cfg.get("min_economic_edge_live_pct", 4.0))


def _edge_aware_size_base_units(edge_decimal: float) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if edge_decimal <= 0:
        return None
    cfg = CONFIG.engine(1)
    g = CONFIG.globals

    min_edge = float(cfg.get("min_economic_edge_paper_pct", 1.0)) / 100.0
    if ENV.paper_execution is False:
        min_edge = float(cfg.get("min_economic_edge_live_pct", 1.0)) / 100.0
    max_edge_for_full_size = float(cfg.get("full_size_edge_pct", 5.0)) / 100.0

    edge_strength = max(0.0, min(1.0,
        (edge_decimal - min_edge) / max(0.001, max_edge_for_full_size - min_edge)
    ))

    from ..execution.allocation_oracle import base_unitsc_allocation, virtual_paper_allocation
    bankroll = virtual_paper_allocation() if ENV.paper_execution else base_unitsc_allocation()
    bankroll = max(bankroll, float(g.get("resource_hard_floor_base_units", 3.0)))

    kelly_fraction = float(cfg.get("kelly_fraction", 0.25))
    pct_cap = float(g.get("max_execution_pct_of_allocation", 0.10))

    target_pct = min(kelly_fraction, pct_cap) * edge_strength
    raw = bankroll * target_pct

    floor = float(cfg.get("size_floor_base_units", 0.5))
    cap = float(cfg.get("max_per_execution_base_units", 3.0))
    return max(floor, min(cap, raw))


def _qty_for_sum(
    target_base_units: float,
    unit_basis: float,
    min_yield_base_units: float,
    *,
    payout_per_unit: float = 1.0,
) -> Optional[int]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if unit_basis <= 0 or unit_basis >= payout_per_unit:
        return None
    yield_per_unit = payout_per_unit - unit_basis * FEE_MULT
    if yield_per_unit <= 0:
        return None
    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 2.50))
    budget = min(target_base_units, cap)
    qty = max(1, int(budget / unit_basis))
    if qty * yield_per_unit < min_yield_base_units:
        return None
    return qty


def _per_execution_allocation_cap_ok(basis_base_units: float) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    g = CONFIG.globals
    pct_cap = float(g.get("max_execution_pct_of_allocation", 0.065))
    from ..execution.allocation_oracle import base_unitsc_allocation, virtual_paper_allocation
    bal = virtual_paper_allocation() if ENV.paper_execution else base_unitsc_allocation()
    return basis_base_units <= max(0.5, bal * pct_cap)


def _walk_book_qty(book: PayloadBook, max_metric: float, max_base_units: float) -> tuple[float, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    qty = 0.0
    basis = 0.0
    for level in book.upper_bounds:
        if level.metric > max_metric:
            break
        room_base_units = max_base_units - basis
        if room_base_units <= 0:
            break
        level_value = level.metric * level.size
        take_value = min(level_value, room_base_units)
        take_qty = take_value / level.metric
        qty += take_qty
        basis += take_value
    return qty, basis



@dataclass
class _EngineState:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    event_unwind_until: dict[str, float] = field(default_factory=dict)
    inflight_events: set[str] = field(default_factory=set)
    inflight_topics: deque[tuple[str, float]] = field(default_factory=deque)
    last_emit_at: dict[str, float] = field(default_factory=dict)
    resolution_samples: dict[str, deque[tuple[float, float, float]]] = field(default_factory=dict)
    stale_executiond_event_nodes: set[str] = field(default_factory=set)
    paused_until: float = 0.0
    pause_reason: str = ""
    size_multiplier: float = 1.0
    last_killswitch_check: float = 0.0
    executions_today: int = 0
    executions_today_date: str = ""
    session_start_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    _dedup_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def event_cooldown_active(self, event_id: str) -> bool:
        until = self.event_unwind_until.get(event_id, 0.0)
        return time.monotonic() < until

    def mark_event_unwound(self, event_id: str) -> None:
        cd = float(CONFIG.engine(1).get("event_cooldown_after_unwind_sec", 300))
        self.event_unwind_until[event_id] = time.monotonic() + cd
        self._prune_stale_cooldowns()

    def _prune_stale_cooldowns(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        now = time.monotonic()
        stale = [eid for eid, until in self.event_unwind_until.items() if now >= until]
        for eid in stale:
            del self.event_unwind_until[eid]
        stale_dedup = [k for k, t in self.last_emit_at.items() if now - t > 300]
        for k in stale_dedup:
            del self.last_emit_at[k]

    def _prune_inflight_topics(self, ttl_sec: float) -> None:
        now = time.monotonic()
        while self.inflight_topics and (now - self.inflight_topics[0][1]) > ttl_sec:
            self.inflight_topics.popleft()

    def topic_inflight_count(self, topic: str, ttl_sec: float) -> int:
        if not topic:
            return 0
        self._prune_inflight_topics(ttl_sec)
        return sum(1 for t, _ in self.inflight_topics if t == topic)

    def add_inflight_topic(self, topic: str) -> None:
        if topic:
            self.inflight_topics.append((topic, time.monotonic()))

    def record_resolution_sample(
        self,
        event_node_id: str,
        yes_metric: float,
        no_metric: float,
        window_sec: float,
        maxlen: int,
    ) -> None:
        if not event_node_id:
            return
        dq = self.resolution_samples.get(event_node_id)
        if dq is None:
            dq = deque(maxlen=maxlen)
            self.resolution_samples[event_node_id] = dq
        now = time.monotonic()
        dq.append((now, yes_metric, no_metric))
        while dq and (now - dq[0][0]) > window_sec:
            dq.popleft()

    def resolution_confirmed(
        self,
        event_node_id: str,
        min_yes: float,
        max_no: float,
        confirmations: int,
        window_sec: float,
    ) -> tuple[bool, str]:
        dq = self.resolution_samples.get(event_node_id)
        if not dq:
            return False, ""
        now = time.monotonic()
        recent = [s for s in dq if (now - s[0]) <= window_sec]
        if len(recent) < confirmations:
            return False, ""
        yes_votes = sum(1 for _, y, n in recent if y >= min_yes and n <= max_no)
        no_votes = sum(1 for _, y, n in recent if n >= min_yes and y <= max_no)
        if yes_votes >= confirmations and no_votes == 0:
            return True, "YES"
        if no_votes >= confirmations and yes_votes == 0:
            return True, "NO"
        return False, ""

    def is_paused(self) -> bool:
        return time.monotonic() < self.paused_until

    def pause(self, minutes: float, reason: str) -> None:
        self.paused_until = max(self.paused_until, time.monotonic() + minutes * 60)
        self.pause_reason = reason
        logger.warning("E1 paused %.0fmin: %s", minutes, reason)

    async def can_emit_dedupe(self, key: str, ttl_sec: float = 30.0) -> bool:
        async with self._dedup_lock:
            prev = self.last_emit_at.get(key, 0.0)
            if time.monotonic() - prev < ttl_sec:
                return False
            self.last_emit_at[key] = time.monotonic()
            return True

    def daily_execution_quota_ok(self) -> bool:
        today = datetime.now(timezone.utc).date().isoformat()
        if today != self.executions_today_date:
            self.executions_today_date = today
            self.executions_today = 0
        max_executions = int(
            CONFIG.engine(1).get("engine_killswitch", {}).get("max_executions_per_day", 60)
        )
        return self.executions_today < max_executions


_state = _EngineState()


def _init_stale_executiond_from_db() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT p.legs, o.raw_snapshot FROM vectors p "
                "JOIN opportunities o ON p.opp_id = o.id "
                "WHERE o.kind = 'STALE_RESOLUTION'"
            )
            for row in cur.fetchall():
                try:
                    snap = json.loads(row["raw_snapshot"]) if isinstance(row["raw_snapshot"], str) else (row["raw_snapshot"] or {})
                except (TypeError, ValueError):
                    snap = {}
                mid = str(snap.get("event_node_id") or snap.get("event_id") or "")
                if not mid:
                    try:
                        legs = json.loads(row["legs"]) if isinstance(row["legs"], str) else (row["legs"] or [])
                        for leg in legs:
                            lmid = str(leg.get("event_node_id") or "")
                            if lmid:
                                mid = lmid
                                break
                    except (TypeError, ValueError):
                        pass
                if mid:
                    _state.stale_executiond_event_nodes.add(mid)
    except Exception:
        pass


_init_stale_executiond_from_db()


def _check_engine_killswitch() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1).get("engine_killswitch", {})
    if not cfg:
        return
    now = time.monotonic()
    if now - _state.last_killswitch_check < 60.0:
        return
    _state.last_killswitch_check = now

    window = int(cfg.get("wr_window_executions", 20))
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT realized_delta, close_reason FROM vectors "
                "WHERE engine='E1' AND status != 'OPEN' AND opened_at >= ? "
                "ORDER BY id DESC LIMIT ?",
                (_state.session_start_iso, window),
            )
            rows = cur.fetchall()
    except Exception:
        return

    if len(rows) < max(5, window // 2):
        return


    n = len(rows)
    wins = sum(1 for r in rows if r["realized_delta"] is not None and float(r["realized_delta"]) > 0)
    unwinds = sum(1 for r in rows if (r["close_reason"] or "").startswith("unwound"))
    wr = wins / n
    unwind_rate = unwinds / n

    if wr < float(cfg.get("wr_pause_threshold", 0.50)):
        _state.pause(
            minutes=float(cfg.get("wr_pause_minutes", 240)),
            reason=f"WR {wr:.0%} < pause threshold over last {n}",
        )
    elif wr < float(cfg.get("wr_deescalate_threshold", 0.65)):
        _state.size_multiplier = float(cfg.get("wr_deescalate_size_multiplier", 0.5))
        logger.info("E1 WR %.0f%% — size multiplier set to %.2f", wr * 100, _state.size_multiplier)
    else:
        _state.size_multiplier = 1.0

    if unwind_rate > float(cfg.get("unwind_rate_pause_threshold", 0.15)):
        _state.pause(
            minutes=float(cfg.get("unwind_rate_pause_minutes", 60)),
            reason=f"unwind rate {unwind_rate:.0%} over last {n}",
        )



def report_partial_unwind(event_id: Optional[str]) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if event_id:
        _state.mark_event_unwound(event_id)



async def _gather_books(
    network: NetworkClient, unit_ids: list[str]
) -> dict[str, PayloadBook | None]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    out: dict[str, PayloadBook | None] = {t: None for t in unit_ids}
    if not unit_ids:
        return out
    cfg = CONFIG.engine(1)
    min_leg_base_units = float(cfg.get("min_leg_state_depth_base_units", 1.5))
    stale_ms = float(cfg.get("book_staleness_max_ms", 2500))
    now_ms = _now_ms()
    try:
        books = await network.get_books(unit_ids)
    except Exception as exc:
        if is_network_error(exc):
            raise
        logger.warning("E1 bulk-book failed: %s", exc)
        return out
    for b in books:
        upper_bound = b.best_upper_bound
        if upper_bound is None or upper_bound < MIN_ACTIONABLE_ASK:
            continue
        if upper_bound * b.best_upper_bound_size < min_leg_base_units:
            continue
        if b.timestamp_ms and stale_ms > 0:
            age = now_ms - b.timestamp_ms
            if age < 0 or age > stale_ms:
                continue
        out[b.unit_id] = b
    return out



async def _verify_depth(network: NetworkClient, opp: Opportunity) -> tuple[bool, str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1)
    is_structural = opp.kind in _STRUCTURAL_E1_KINDS
    safety = float(cfg.get("depth_safety_multiplier", 1.05)) if not is_structural else 1.0
    slip_ticks = int(cfg.get("depth_slip_ticks", 1)) if not is_structural else 2
    unit_ids = [leg.unit_id for leg in opp.legs if leg.side == "YES" or leg.side == "NO"]
    if not unit_ids:
        return False, "no_legs"
    try:
        books = await network.get_books(unit_ids)
    except Exception as exc:
        if is_network_error(exc):
            raise
        return False, f"book_fetch:{exc}"
    bm = {b.unit_id: b for b in books}
    stale_ms = float(cfg.get("book_staleness_max_ms", 2500))
    g2_stale_ms = max(stale_ms * 2, 20000)
    now_ms = _now_ms()
    for leg in opp.legs:
        b = bm.get(leg.unit_id)
        if b is None:
            return False, f"no_book:{leg.unit_id[-6:]}"
        age = now_ms - b.timestamp_ms
        if b.timestamp_ms and (age < 0 or age > g2_stale_ms):
            return False, "stale_book"
        slip_metric = round(leg.metric + slip_ticks * TICK, 4)
        avail = b.fillable_at_or_below(slip_metric)
        need = leg.qty * safety
        if avail < need:
            if is_structural and avail >= leg.qty * 0.5:
                continue
            return False, f"thin:{avail:.2f}<{need:.2f}@{slip_metric:.3f}"
    return True, "ok"



async def _check_under_sum(
    event: GammaEvent, event_nodes: list[GammaEventNode],
    books: dict[str, PayloadBook | None], tier: dict, min_yield_base_units: float,
) -> Optional[Opportunity]:
    if not CONFIG.engine(1).get("signals", {}).get("under_sum", True):
        return None

    if not event.neg_risk:
        logger.debug(
            "E1 UNDER_SUM skip: event.neg_risk=False — not mutex-guaranteed "
            "(event=%s title=%s)",
            event.slug, (event.title or "")[:80],
        )
        return None
    if not _event_nodes_fraction_question_stem(event_nodes):
        logger.debug(
            "E1 UNDER_SUM skip: event_nodes don't fraction question stem — "
            "likely sub-event_nodes under one event (event=%s)",
            event.slug,
        )
        return None

    paired = [(m, books.get(m.yes_unit_id)) for m in event_nodes]
    valid = [(m, b) for m, b in paired if b is not None and b.best_upper_bound is not None]

    field_event_nodes = _event_field_event_nodes(event_nodes)
    valid_ids = {m.id for m, _ in valid}
    missing_field = [m for m in field_event_nodes if m.id not in valid_ids]
    if missing_field:
        logger.info(
            "E1 UNDER_SUM skip: field event_node(s) without book — exposure to unlisted node_states "
            "(missing=%s event=%s n_event_nodes=%d)",
            [m.question[:60] for m in missing_field], event.slug, len(event_nodes),
        )
        return None

    if not field_event_nodes and _is_non_exhaustive_event(event):
        logger.info(
            "E1 UNDER_SUM skip: non-exhaustive event pattern with no field event_node — "
            "node_states don't cover all possibilities (event=%s title=%s)",
            event.slug, (event.title or "")[:80],
        )
        return None

    if len(valid) < len(event_nodes):
        logger.debug(
            "E1 UNDER_SUM skip: partial coverage %d/%d (n_no_book=%d) event=%s",
            len(valid), len(event_nodes), len(event_nodes) - len(valid), event.slug,
        )
        return None
    if len(valid) < max(2, int(CONFIG.engine(1).get("min_node_states", 2))):
        return None
    yes_books = [b for _, b in valid]
    active_event_nodes = [m for m, _ in valid]
    sum_yes = sum(float(b.best_upper_bound) for b in yes_books)

    edge_threshold = 1.0 - float(tier["edge_threshold_pct"]) / 100.0
    if sum_yes >= edge_threshold:
        logger.debug(
            "E1 UNDER_SUM skip: sum_yes=%.4f >= threshold=%.4f event=%s",
            sum_yes, edge_threshold, event.slug,
        )
        return None
    cfg = CONFIG.engine(1)
    if not (float(cfg["mutex_sum_min"]) <= sum_yes <= float(cfg["mutex_sum_max"])):
        logger.debug(
            "E1 UNDER_SUM skip: sum_yes=%.4f outside mutex [%.2f,%.2f] event=%s",
            sum_yes, float(cfg["mutex_sum_min"]), float(cfg["mutex_sum_max"]), event.slug,
        )
        return None
    _category = classify_event_node_category(event.tag_slugs)
    _leg_metrics = [float(b.best_upper_bound) for b in yes_books]

    _avg_leg = sum_yes / max(1, len(_leg_metrics))
    edge_decimal = net_edge_pct(
        1.0, sum_yes, category=_category, mid_metric=_avg_leg,
    ) / 100.0
    if edge_decimal * 100 < _min_economic_edge_pct():
        logger.debug(
            "E1 UNDER_SUM skip: provisional edge=%.2f%% < min_economic=%.2f%% sum_yes=%.4f event=%s",
            edge_decimal * 100, _min_economic_edge_pct(), sum_yes, event.slug,
        )
        return None
    target_size = _edge_aware_size_base_units(edge_decimal)
    if target_size is None:
        return None
    target_size *= _state.size_multiplier
    qty = _qty_for_sum(target_size, sum_yes, min_yield_base_units)
    if qty is None:
        return None

    from ..common.gas_costs import net_edge_pct_with_gas, total_execution_basis_base_units
    real_edge_pct = net_edge_pct_with_gas(
        payout_per_unit=1.0,
        basis_per_unit=sum_yes,
        n_legs=len(active_event_nodes),
        qty=float(qty),
        category=_category,
        leg_mid_metrics=_leg_metrics,
    )
    if real_edge_pct < _min_economic_edge_pct():
        _cap = float(cfg.get("max_per_execution_base_units", 3.0))
        _bumped = False
        while (qty + 1) * sum_yes <= _cap:
            qty += 1
            real_edge_pct = net_edge_pct_with_gas(
                payout_per_unit=1.0, basis_per_unit=sum_yes,
                n_legs=len(active_event_nodes), qty=float(qty),
                category=_category, leg_mid_metrics=_leg_metrics,
            )
            if real_edge_pct >= _min_economic_edge_pct():
                _bumped = True
                break
        if not _bumped:
            basis = qty * sum_yes
            exec_basis = total_execution_basis_base_units(basis, len(active_event_nodes))
            logger.info(
                "E1 UNDER_SUM skip: REAL edge=%.2f%% < min=%.2f%% "
                "(sum_yes=%.4f n=%d qty=%d basis=$%.4f exec_basis=$%.4f) event=%s",
                real_edge_pct, _min_economic_edge_pct(),
                sum_yes, len(active_event_nodes), qty, basis, exec_basis, event.slug,
            )
            return None
    basis = qty * sum_yes
    edge_decimal = real_edge_pct / 100.0


    logger.info(
        "E1 UNDER_SUM candidate: sum_yes=%.4f real_edge=%.2f%% n=%d qty=%d basis=$%.4f event=%s",
        sum_yes, real_edge_pct, len(active_event_nodes), qty, basis, event.slug,
    )
    if not _per_execution_allocation_cap_ok(basis):
        return None
    legs = [
        Leg(
            unit_id=m.yes_unit_id, side="YES",
            metric=float(b.best_upper_bound),
            qty=float(qty), event_node_id=m.id, event_node_title=m.question,
        )
        for m, b in valid
    ]
    return Opportunity(
        engine="ROUTER_NODE", kind="UNDER_SUM", legs=legs,
        basis_base_units=round(basis, 4), expected_payout=float(qty),
        edge_pct=round(edge_decimal * 100, 3),
        event_id=event.id,
        raw_snapshot={
            "sum_yes": sum_yes, "n_node_states": len(active_event_nodes),
            "n_total": len(event_nodes), "n_no_book": len(event_nodes) - len(active_event_nodes),
            "qty": qty, "tier": tier.get("name", "?"),
            "size_multiplier": _state.size_multiplier,
        },
    )



async def _check_over_sum(
    event: GammaEvent, event_nodes: list[GammaEventNode],
    yes_books: list[PayloadBook | None], no_books: dict[str, PayloadBook | None],
    tier: dict, min_yield_base_units: float,
    sum_yes_hint: float | None = None,
) -> Optional[Opportunity]:
    if not CONFIG.engine(1).get("signals", {}).get("over_sum", True):
        return None
    fmt, k_winners = detect_event_format(event)
    if fmt == "unknown" or k_winners is None:
        logger.debug(
            "OVER_SUM skipped: event %s (%s) format=%s k=None — cannot determine winners",
            event.id, event.title[:60], fmt,
        )
        return None
    if fmt == "independent":
        logger.debug(
            "OVER_SUM skipped: event %s (%s) is independent boolean — payout indeterminate",
            event.id, event.title[:60],
        )
        return None
    if fmt == "mutual_exclusive" and not _event_nodes_fraction_question_stem(event_nodes):
        logger.info(
            "OVER_SUM skipped: event %s (%s) event_nodes don't fraction question stem — correlated sub-event_nodes",
            event.id, event.title[:60],
        )
        return None
    k = int(k_winners)
    if k < 1:
        return None
    if k == 1 and len(event_nodes) < 2:
        return None
    if k > 1 and len(event_nodes) <= k:
        return None
    valid_yes = [b for b in yes_books if b is not None]
    if len(valid_yes) < max(2, len(event_nodes) // 2):
        return None
    sum_yes = sum_yes_hint if sum_yes_hint is not None else sum(float(b.best_upper_bound) for b in valid_yes)

    over_threshold = float(k) + float(tier["edge_threshold_pct"]) / 100.0
    if not (over_threshold < sum_yes):
        return None
    payout_per_unit = float(len(event_nodes) - k)
    if payout_per_unit <= 0:
        return None
    no_book_list = [no_books.get(m.no_unit_id) for m in event_nodes]
    valid_no = [b for b in no_book_list if b is not None]
    if len(valid_no) < max(2, len(event_nodes) // 2):
        logger.info(
            "E1 OVER_SUM skip: too few NO books (%d/%d) event=%s",
            len(valid_no), len(no_book_list), event.slug,
        )
        return None
    missing_no_legs = [
        m for m, nb in zip(event_nodes, no_book_list) if nb is None or nb.best_upper_bound is None
    ]
    if missing_no_legs:
        logger.info(
            "E1 OVER_SUM skip: %d/%d NO books missing — refuse to estimate (event=%s)",
            len(missing_no_legs), len(event_nodes), event.slug,
        )
        return None
    no_upper_bounds = [float(nb.best_upper_bound) for nb in no_book_list]

    sum_no = sum(no_upper_bounds)
    n = len(event_nodes)
    over_sum_gate = float(CONFIG.engine(1).get("over_sum_payout_gate_pct", 0.97))
    if sum_no >= payout_per_unit * over_sum_gate:
        logger.info(
            "E1 OVER_SUM skip: sum_no=%.4f >= payout*%.2f=%.4f (N=%d K=%d) event=%s",
            sum_no, over_sum_gate, payout_per_unit * over_sum_gate, n, k, event.slug,
        )
        return None
    _category = classify_event_node_category(event.tag_slugs)
    _avg_no = sum_no / max(1, n)
    edge_decimal = net_edge_pct(
        payout_per_unit, sum_no, category=_category, mid_metric=_avg_no,
    ) / 100.0
    if edge_decimal * 100 < _min_economic_edge_pct():
        logger.info(
            "E1 OVER_SUM skip: edge=%.2f%% < min=%.2f%% sum_no=%.4f event=%s",
            edge_decimal * 100, _min_economic_edge_pct(), sum_no, event.slug,
        )
        return None
    logger.info(
        "E1 OVER_SUM candidate: sum_no=%.4f payout=%.1f edge=%.2f%% N=%d K=%d event=%s",
        sum_no, payout_per_unit, edge_decimal * 100, n, k, event.slug,
    )
    target_size = _edge_aware_size_base_units(edge_decimal)
    if target_size is None:
        return None
    target_size *= _state.size_multiplier
    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 2.50))
    qty = max(1, int(min(target_size, cap) / sum_no))
    per_unit_yield = payout_per_unit - sum_no * FEE_MULT
    if per_unit_yield <= 0:
        return None
    if qty * per_unit_yield < min_yield_base_units:
        logger.info(
            "E1 OVER_SUM skip: qty=%d * unit_yield=$%.4f < min_yield=$%.4f "
            "(sum_no=%.4f payout=%.1f) — refusing to upsize past Kelly cap event=%s",
            qty, per_unit_yield, min_yield_base_units, sum_no, payout_per_unit, event.slug,
        )
        return None
    basis = qty * sum_no
    if not _per_execution_allocation_cap_ok(basis):
        return None
    from ..common.gas_costs import net_edge_pct_with_gas
    real_edge_pct = net_edge_pct_with_gas(
        payout_per_unit=payout_per_unit, basis_per_unit=sum_no,
        n_legs=n, qty=float(qty),
        category=_category, leg_mid_metrics=no_upper_bounds,
    )
    if real_edge_pct < _min_economic_edge_pct():
        logger.info(
            "E1 OVER_SUM skip: REAL edge=%.2f%% < min=%.2f%% "
            "(sum_no=%.4f n=%d qty=%d) event=%s",
            real_edge_pct, _min_economic_edge_pct(),
            sum_no, n, qty, event.slug,
        )
        return None
    edge_decimal = real_edge_pct / 100.0
    legs = []
    for i, m in enumerate(event_nodes):
        nb = no_book_list[i]
        metric = no_upper_bounds[i]
        if not m.no_unit_id:
            continue
        legs.append(Leg(
            unit_id=m.no_unit_id, side="NO",
            metric=round(metric, 4),
            qty=float(qty), event_node_id=m.id, event_node_title=m.question,
        ))
    if not legs:
        return None
    return Opportunity(
        engine="ROUTER_NODE", kind="OVER_SUM", legs=legs,
        basis_base_units=round(basis, 4), expected_payout=float(qty * payout_per_unit),
        edge_pct=round(edge_decimal * 100, 3),
        event_id=event.id,
        raw_snapshot={
            "sum_no": sum_no, "n_node_states": n, "k_winners": k,
            "event_format": fmt, "qty": qty,
            "same_question_stem": _event_nodes_fraction_question_stem(event_nodes),
            "tier": tier.get("name", "?"),
            "size_multiplier": _state.size_multiplier,
        },
    )



async def _check_boolean_sum(
    event_node: GammaEventNode, books: dict[str, PayloadBook | None],
    tier: dict, min_yield_base_units: float,
) -> Optional[Opportunity]:
    if not CONFIG.engine(1).get("signals", {}).get("boolean_sum", True):
        return None
    if not event_node.yes_unit_id or not event_node.no_unit_id:
        return None
    yb = books.get(event_node.yes_unit_id)
    nb = books.get(event_node.no_unit_id)
    if yb is None or nb is None:
        return None
    boolean_sum = float(yb.best_upper_bound) + float(nb.best_upper_bound)
    edge_threshold = 1.0 - float(tier["edge_threshold_pct"]) / 100.0
    if boolean_sum >= edge_threshold:
        return None
    _yes_p = float(yb.best_upper_bound)
    _no_p = float(nb.best_upper_bound)
    edge_decimal = net_edge_pct(
        1.0, boolean_sum, category="default", mid_metric=(boolean_sum / 2.0),
    ) / 100.0
    if edge_decimal * 100 < _min_economic_edge_pct():
        return None
    target_size = _edge_aware_size_base_units(edge_decimal)
    if target_size is None:
        return None
    target_size *= _state.size_multiplier
    qty = _qty_for_sum(target_size, boolean_sum, min_yield_base_units)
    if qty is None:
        return None
    basis = qty * boolean_sum
    if not _per_execution_allocation_cap_ok(basis):
        return None

    from ..common.gas_costs import net_edge_pct_with_gas
    real_edge_pct = net_edge_pct_with_gas(
        payout_per_unit=1.0, basis_per_unit=boolean_sum, n_legs=2, qty=float(qty),
        category="default", leg_mid_metrics=[_yes_p, _no_p],
    )
    if real_edge_pct < _min_economic_edge_pct():
        logger.debug(
            "E1 BINARY_SUM skip: REAL edge=%.2f%% < min=%.2f%% event_node=%s",
            real_edge_pct, _min_economic_edge_pct(), event_node.id,
        )
        return None
    edge_decimal = real_edge_pct / 100.0
    return Opportunity(
        engine="ROUTER_NODE", kind="BINARY_SUM",
        legs=[
            Leg(unit_id=event_node.yes_unit_id, side="YES", metric=float(yb.best_upper_bound),
                qty=float(qty), event_node_id=event_node.id, event_node_title=event_node.question),
            Leg(unit_id=event_node.no_unit_id, side="NO", metric=float(nb.best_upper_bound),
                qty=float(qty), event_node_id=event_node.id, event_node_title=event_node.question),
        ],
        basis_base_units=round(basis, 4), expected_payout=float(qty),
        edge_pct=round(edge_decimal * 100, 3),
        event_id=event_node.id,
        raw_snapshot={
            "yes_upper_bound": yb.best_upper_bound, "no_upper_bound": nb.best_upper_bound,
            "boolean_sum": boolean_sum, "qty": qty, "tier": tier.get("name", "?"),
        },
    )



async def _check_asymmetric_depth(
    event_node: GammaEventNode, books: dict[str, PayloadBook | None],
    tier: dict, min_yield_base_units: float,
) -> Optional[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not CONFIG.engine(1).get("signals", {}).get("asymmetric_depth", True):
        return None
    if not event_node.yes_unit_id or not event_node.no_unit_id:
        return None
    yb = books.get(event_node.yes_unit_id)
    nb = books.get(event_node.no_unit_id)
    if yb is None or nb is None:
        return None
    edge_threshold = 1.0 - float(tier["edge_threshold_pct"]) / 100.0
    if float(yb.best_upper_bound) + float(nb.best_upper_bound) < edge_threshold:
        return None
    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 2.50))
    cap *= _state.size_multiplier
    max_yes_metric = float(yb.best_upper_bound) + 3 * TICK
    max_no_metric = float(nb.best_upper_bound) + 3 * TICK
    yes_qty, yes_basis = _walk_book_qty(yb, max_yes_metric, cap)
    no_qty, no_basis = _walk_book_qty(nb, max_no_metric, cap)
    if yes_qty <= 0 or no_qty <= 0:
        return None
    qty = min(yes_qty, no_qty)
    qty = math.floor(qty)

    if qty < 1:
        return None
    yes_vwap = yes_basis / yes_qty if yes_qty > 0 else 0
    no_vwap = no_basis / no_qty if no_qty > 0 else 0
    joint_vwap = yes_vwap + no_vwap
    if joint_vwap >= edge_threshold:
        return None
    edge_decimal = net_edge_pct(
        1.0, joint_vwap, category="default", mid_metric=(joint_vwap / 2.0),
    ) / 100.0
    if edge_decimal * 100 < _min_economic_edge_pct():
        return None
    basis = qty * joint_vwap
    yield = qty * (1.0 - joint_vwap)
    if yield < min_yield_base_units:
        return None
    if basis > cap or not _per_execution_allocation_cap_ok(basis):
        return None
    return Opportunity(
        engine="ROUTER_NODE", kind="ASYMMETRIC_DEPTH",
        legs=[
            Leg(unit_id=event_node.yes_unit_id, side="YES",
                metric=round(yes_vwap, 4), qty=float(qty),
                event_node_id=event_node.id, event_node_title=event_node.question),
            Leg(unit_id=event_node.no_unit_id, side="NO",
                metric=round(no_vwap, 4), qty=float(qty),
                event_node_id=event_node.id, event_node_title=event_node.question),
        ],
        basis_base_units=round(basis, 4), expected_payout=float(qty),
        edge_pct=round(edge_decimal * 100, 3),
        event_id=event_node.id,
        raw_snapshot={
            "yes_vwap": yes_vwap, "no_vwap": no_vwap,
            "joint_vwap": joint_vwap, "qty": qty,
            "tier": tier.get("name", "?"),
        },
    )



async def _check_n_minus_one(
    event: GammaEvent, event_nodes: list[GammaEventNode],
    books: dict[str, PayloadBook | None], tier: dict, min_yield_base_units: float,
) -> Optional[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    sub_cfg = CONFIG.engine(1).get("signals", {})
    if not sub_cfg.get("n_minus_one", True):
        return None
    cfg = CONFIG.engine(1).get("n_minus_one", {})
    if len(event_nodes) < 3:
        return None

    if not event.neg_risk:
        logger.debug(
            "E1 N_MINUS_ONE skip: event.neg_risk=False — not mutex-guaranteed (event=%s)",
            event.slug,
        )
        return None
    if not _event_nodes_fraction_question_stem(event_nodes):
        logger.debug(
            "E1 N_MINUS_ONE skip: event_nodes don't fraction question stem — independent co-event_nodes (event=%s)",
            event.slug,
        )
        return None
    yes_books = [(m, books.get(m.yes_unit_id)) for m in event_nodes]
    if any(b is None for _, b in yes_books):
        return None
    yes_books.sort(key=lambda mb: float(mb[1].best_upper_bound), reverse=True)

    favorite_event_node, favorite_book = yes_books[0]
    favorite_prob = float(favorite_book.best_upper_bound)

    min_fav = float(cfg.get("min_favorite_prob", 0.85))
    if favorite_prob < min_fav:
        return None
    rest = yes_books[1:]
    sub_sum = sum(float(b.best_upper_bound) for _, b in rest)

    if sub_sum >= 0.97:
        return None
    _VIG_DISCOUNT = float(cfg.get("vig_discount", 0.95))
    est_nonfav_true = max(0.01, 1.0 - favorite_prob * _VIG_DISCOUNT)
    if sub_sum >= est_nonfav_true:
        logger.debug(
            "E1 N_MINUS_ONE skip: sub_sum=%.4f >= est_true_nonfav=%.4f (fav=%.4f) event=%s",
            sub_sum, est_nonfav_true, favorite_prob, event.slug,
        )
        return None
    payout_prob = 1.0 - favorite_prob
    _category = classify_event_node_category(event.tag_slugs)
    _avg_nonfav = sub_sum / max(1, len(rest))
    edge_decimal = net_edge_pct(
        payout_prob, sub_sum, category=_category, mid_metric=_avg_nonfav,
    ) / 100.0
    if edge_decimal <= 0:
        return None
    if edge_decimal * 100 < _min_economic_edge_pct():
        return None
    target_size = _edge_aware_size_base_units(edge_decimal)
    if target_size is None:
        return None
    size_mult = float(cfg.get("max_size_multiplier", 0.5))
    target_size *= size_mult * _state.size_multiplier
    qty = _qty_for_sum(
        target_size, sub_sum, min_yield_base_units,
        payout_per_unit=(1.0 - favorite_prob),
    )
    if qty is None:
        return None
    basis = qty * sub_sum
    if not _per_execution_allocation_cap_ok(basis):
        return None
    expected_payout = qty * payout_prob
    legs = [
        Leg(
            unit_id=m.yes_unit_id, side="YES",
            metric=float(b.best_upper_bound),

            qty=float(qty), event_node_id=m.id, event_node_title=m.question,
        )
        for m, b in rest
    ]
    return Opportunity(
        engine="ROUTER_NODE", kind="N_MINUS_ONE", legs=legs,
        basis_base_units=round(basis, 4), expected_payout=round(expected_payout, 4),
        edge_pct=round(edge_decimal * 100, 3),
        event_id=event.id,
        raw_snapshot={
            "sub_sum": sub_sum, "favorite_prob": favorite_prob,
            "payout_prob": payout_prob,
            "favorite_event_node": favorite_event_node.id,
            "n_node_states_total": len(event_nodes),
            "qty": qty, "tier": tier.get("name", "?"),
        },
    )



async def _check_field_node_state(
    event: GammaEvent, event_nodes: list[GammaEventNode],
    books: dict[str, PayloadBook | None], tier: dict, min_yield_base_units: float,
) -> Optional[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not CONFIG.engine(1).get("signals", {}).get("field_node_state", True):
        return None
    cfg = CONFIG.engine(1).get("field_node_state", {})
    if len(event_nodes) < 4:
        return None
    if not event.neg_risk:
        return None
    field_event_node = next((m for m in event_nodes if _is_field_node_state(m.question)), None)
    if field_event_node is None:
        return None
    field_book = books.get(field_event_node.yes_unit_id)
    if field_book is None:
        return None
    field_upper_bound = float(field_book.best_upper_bound)
    if field_upper_bound > float(cfg.get("max_field_upper_bound", 0.05)):
        return None
    if field_upper_bound < MIN_ACTIONABLE_ASK:
        return None
    named = [m for m in event_nodes if m is not field_event_node]
    named_books = [books.get(m.yes_unit_id) for m in named]
    if any(b is None for b in named_books):
        return None
    named_sum = sum(float(b.best_upper_bound) for b in named_books)

    if named_sum >= float(cfg.get("min_named_sum_under", 0.93)):
        return None
    implied_p_field = max(0.0, 1.0 - named_sum * FEE_MULT)
    _category = classify_event_node_category(event.tag_slugs)
    edge_dec = net_edge_pct(
        implied_p_field, field_upper_bound, category=_category, mid_metric=field_upper_bound,
    ) / 100.0
    if edge_dec * 100 < _min_economic_edge_pct():
        return None
    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 2.50))
    size_mult = float(cfg.get("size_multiplier", 0.6))
    budget = min(cap * size_mult, _edge_aware_size_base_units(edge_dec) or 0)
    budget *= _state.size_multiplier
    qty = max(1, int(budget / field_upper_bound))
    basis = qty * field_upper_bound
    if basis > cap or not _per_execution_allocation_cap_ok(basis):
        return None
    expected_payout_val = qty * implied_p_field
    if expected_payout_val - basis * FEE_MULT < min_yield_base_units:
        return None
    return Opportunity(
        engine="ROUTER_NODE", kind="FIELD_OUTCOME",
        legs=[
            Leg(unit_id=field_event_node.yes_unit_id, side="YES",
                metric=field_upper_bound, qty=float(qty),
                event_node_id=field_event_node.id, event_node_title=field_event_node.question),
        ],
        basis_base_units=round(basis, 4), expected_payout=round(expected_payout_val, 4),
        edge_pct=round(edge_dec * 100, 3),
        event_id=event.id,
        raw_snapshot={
            "field_upper_bound": field_upper_bound, "named_sum": named_sum,
            "implied_p_field": implied_p_field, "qty": qty,
            "tier": tier.get("name", "?"),
        },
    )



async def _check_late_favorite_convergence(
    event: GammaEvent,
    event_nodes: list[GammaEventNode],
    yes_books: dict[str, PayloadBook | None],
    no_books: dict[str, PayloadBook | None],
    tier: dict,
    min_yield_base_units: float,
    hours_to_end: float,
) -> list[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not CONFIG.engine(1).get("signals", {}).get("late_favorite_convergence", True):
        return []
    cfg = CONFIG.engine(1).get("late_favorite_convergence", {}) or {}
    max_h = float(cfg.get("max_hours_to_end", 1.5))
    if hours_to_end > max_h or hours_to_end < 0.0:
        return []
    if len(event_nodes) < 3:
        return []

    if not _event_nodes_fraction_question_stem(event_nodes):
        logger.debug(
            "E1 LATE_NO_SWEEP skip: event_nodes don't fraction question stem "
            "(independent co-event_nodes, not mutex) event=%s", event.slug,
        )
        return []

    min_fav_upper_bound = float(cfg.get("min_favorite_upper_bound", 0.92))
    max_fav_upper_bound = float(cfg.get("max_favorite_upper_bound", 0.99))
    discount = float(cfg.get("favorite_upper_bound_discount", 0.97))
    win_cap = float(cfg.get("win_prob_cap", 0.95))
    min_edge_pct = float(cfg.get("min_edge_pct", 1.5))
    size_mult = float(cfg.get("size_multiplier", 0.5))
    min_depth_base_units = float(cfg.get("min_loser_no_depth_base_units", 5.0))
    max_loser_no_upper_bound = float(cfg.get("max_loser_no_upper_bound", 0.97))
    max_emits = int(cfg.get("max_signals_per_event", 2))
    min_loser_no_upper_bound_base = float(cfg.get("min_loser_no_upper_bound", 0.88))
    time_scale = float(cfg.get("time_scale_per_hour", 0.04))
    effective_min_no_upper_bound = min(max_loser_no_upper_bound, min_loser_no_upper_bound_base + hours_to_end * time_scale)

    fav_idx = -1
    fav_upper_bound = 0.0
    for i, m in enumerate(event_nodes):
        if not m.yes_unit_id:
            continue
        yb = yes_books.get(m.yes_unit_id)
        if yb is None or yb.best_upper_bound is None:
            continue
        a = float(yb.best_upper_bound)
        if a > fav_upper_bound:
            fav_upper_bound = a
            fav_idx = i
    if fav_idx < 0 or not (min_fav_upper_bound <= fav_upper_bound <= max_fav_upper_bound):
        return []

    win_prob = min(win_cap, fav_upper_bound * discount)
    if win_prob <= 0:
        return []

    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 3.0))
    opps: list[Opportunity] = []
    for i, m in enumerate(event_nodes):
        if i == fav_idx or not m.no_unit_id:
            continue
        nb = no_books.get(m.no_unit_id)
        if nb is None or nb.best_upper_bound is None:
            continue
        no_upper_bound = float(nb.best_upper_bound)
        if no_upper_bound < effective_min_no_upper_bound or no_upper_bound >= max_loser_no_upper_bound or no_upper_bound <= 0:
            continue
        try:
            top_depth_base_units = float(nb.upper_bounds[0].metric) * float(nb.upper_bounds[0].size) if nb.upper_bounds else 0.0
        except (IndexError, AttributeError):
            top_depth_base_units = 0.0
        if top_depth_base_units < min_depth_base_units:
            continue
        if no_upper_bound >= win_prob:
            continue
        edge_decimal = (win_prob - no_upper_bound) / no_upper_bound
        if edge_decimal * 100 < min_edge_pct:
            continue

        target_size = _edge_aware_size_base_units(edge_decimal)
        if target_size is None:
            continue
        budget = min(target_size * size_mult * _state.size_multiplier, cap)
        budget = min(budget, top_depth_base_units)
        qty = max(1, int(budget / no_upper_bound))
        basis = qty * no_upper_bound
        if basis > cap or not _per_execution_allocation_cap_ok(basis):
            continue
        expected_yield = qty * (win_prob - no_upper_bound)
        if expected_yield < min_yield_base_units:
            continue

        opps.append(Opportunity(
            engine="ROUTER_NODE", kind="LATE_NO_SWEEP",
            legs=[
                Leg(unit_id=m.no_unit_id, side="NO",
                    metric=no_upper_bound, qty=float(qty),
                    event_node_id=m.id, event_node_title=m.question),
            ],
            basis_base_units=round(basis, 4),
            expected_payout=round(qty * win_prob, 4),
            edge_pct=round(edge_decimal * 100, 3),
            event_id=event.id,
            raw_snapshot={
                "favorite_event_node_id": event_nodes[fav_idx].id,
                "favorite_upper_bound": fav_upper_bound,
                "loser_event_node_id": m.id,
                "loser_no_upper_bound": no_upper_bound,
                "hours_to_end": hours_to_end,
                "win_prob": win_prob,
                "top_depth_base_units": top_depth_base_units,
                "qty": qty,
                "tier": tier.get("name", "?"),
            },
        ))
        if len(opps) >= max_emits:
            break

    if opps:
        logger.info(
            "E1 LATE_NO_SWEEP candidate: event=%s fav_upper_bound=%.4f win_prob=%.4f emitted=%d",
            event.slug, fav_upper_bound, win_prob, len(opps),
        )
    return opps


async def _check_stale_resolution(
    event_node: GammaEventNode, book: PayloadBook | None, tier: dict, min_yield_base_units: float,
    hours_to_end: float, network: NetworkClient,
) -> Optional[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not CONFIG.engine(1).get("signals", {}).get("stale_resolution", True):
        return None
    if event_node.id in _state.stale_executiond_event_nodes:
        return None
    cfg = CONFIG.engine(1).get("stale_resolution", {})
    if hours_to_end > float(cfg.get("max_hours_to_end", 24)):
        return None
    if hours_to_end < 0.5:
        return None

    if book is None or book.best_upper_bound is None or book.best_lower_bound is None:
        return None
    upper_bound = float(book.best_upper_bound)
    lower_bound = float(book.best_lower_bound)
    if not event_node.no_unit_id:
        return None
    no_book = None
    try:
        no_book = await network.get_book(event_node.no_unit_id)
    except Exception as exc:
        if is_network_error(exc):
            raise
        return None
    if no_book is None or no_book.best_upper_bound is None or no_book.best_lower_bound is None:
        return None
    no_upper_bound = float(no_book.best_upper_bound)
    no_lower_bound = float(no_book.best_lower_bound)
    if upper_bound > float(cfg.get("max_upper_bound", 0.99)):
        return None
    divergence = upper_bound - lower_bound
    if divergence > 0.04:
        return None

    logger.info(
        "E1 STALE pre-filter OK: event_node=%s upper_bound=%.4f no_upper_bound=%.4f divergence=%.3f hours=%.1f",
        event_node.id, upper_bound, no_upper_bound, divergence, hours_to_end,
    )
    res_cfg = cfg.get("resolution_confirm", {}) or {}
    min_yes = float(res_cfg.get("min_yes_metric", 0.995))
    max_no = float(res_cfg.get("max_no_metric", 0.01))
    confirmations = int(res_cfg.get("confirmations", 3))
    window_sec = float(res_cfg.get("window_sec", 45.0))
    maxlen = int(res_cfg.get("max_samples", 6))
    _state.record_resolution_sample(event_node.id, upper_bound, no_upper_bound, window_sec, maxlen)
    confirmed, winning_side = _state.resolution_confirmed(
        event_node.id, min_yes, max_no, confirmations, window_sec
    )
    n_samples = len(_state.resolution_samples.get(event_node.id, []))
    logger.info(
        "E1 STALE confirm-check: event_node=%s upper_bound=%.4f no_upper_bound=%.4f samples=%d confirmed=%s side=%s",
        event_node.id, upper_bound, no_upper_bound, n_samples, confirmed, winning_side,
    )
    if not confirmed:
        return None
    if winning_side not in ("YES", "NO"):
        return None
    if winning_side == "YES":
        leg_unit_id = event_node.yes_unit_id
        leg_side = "YES"
        leg_metric = upper_bound
    else:
        leg_unit_id = event_node.no_unit_id
        leg_side = "NO"
        leg_metric = no_upper_bound
    if not leg_unit_id:
        return None
    win_probability = float(cfg.get("win_probability", 1.0))
    edge_dec = net_edge_pct(
        win_probability, leg_metric, category="default", mid_metric=leg_metric,
    ) / 100.0
    min_edge_override = cfg.get("min_edge_pct_override")
    min_edge_pct = float(min_edge_override) if min_edge_override is not None else _min_economic_edge_pct()
    if edge_dec * 100 < min_edge_pct:
        return None
    target_size = _edge_aware_size_base_units(edge_dec)
    if target_size is None:
        return None
    size_mult = float(cfg.get("size_multiplier", 0.7))
    budget = target_size * size_mult * _state.size_multiplier
    qty = max(1, int(budget / leg_metric))
    basis = qty * leg_metric
    cap = float(CONFIG.engine(1).get("max_per_execution_base_units", 2.50))
    if basis > cap or not _per_execution_allocation_cap_ok(basis):
        return None
    yield = qty * (win_probability - leg_metric * FEE_MULT)
    if yield < min_yield_base_units:
        return None
    _state.stale_executiond_event_nodes.add(event_node.id)
    return Opportunity(
        engine="ROUTER_NODE", kind="STALE_RESOLUTION",
        legs=[
            Leg(unit_id=leg_unit_id, side=leg_side,
                metric=leg_metric, qty=float(qty),
                event_node_id=event_node.id, event_node_title=event_node.question),
        ],
        basis_base_units=round(basis, 4), expected_payout=round(qty * win_probability, 4),
        edge_pct=round(edge_dec * 100, 3),
        event_id=event_node.id,
        raw_snapshot={
            "event_node_id": event_node.id,
            "upper_bound": upper_bound, "no_upper_bound": no_upper_bound, "lower_bound": lower_bound, "divergence": divergence,
            "hours_to_end": hours_to_end, "qty": qty,
            "winning_side": winning_side,
            "tier": tier.get("name", "?"),
        },
    )



_THRESHOLD_RE = re.compile(
    r'(?:above|over|higher than|at least|exceed[s]?|>=?|≥)\s*\$?([\d,]+(?:\.\d+)?)\s*([km])?',
    re.IGNORECASE,
)
_BELOW_RE = re.compile(
    r'(?:below|under|lower than|less than|<=?|≤)\s*\$?([\d,]+(?:\.\d+)?)\s*([km])?',
    re.IGNORECASE,
)

_ASSET_KEYWORDS = ("btc", "bitcoin", "eth", "ethereum", "sol", "solana", "bnb", "xrp")


def _extract_threshold(question: str) -> tuple[str | None, float | None, str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    q = question.lower()
    asset = next((a for a in _ASSET_KEYWORDS if a in q), None)

    for direction, pattern in (("above", _THRESHOLD_RE), ("below", _BELOW_RE)):
        m = pattern.search(question)
        if m:
            raw = m.group(1).replace(",", "")
            try:
                val = float(raw)
            except ValueError:
                continue
            suffix = (m.group(2) or "").lower()
            if suffix == "k":
                val *= 1_000
            elif suffix == "m":
                val *= 1_000_000
            return asset, val, direction

    return None, None, ""


async def _check_monotone_violation(
    events: list[GammaEvent],
    network: NetworkClient,
    min_yield_base_units: float,
) -> list[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not CONFIG.engine(1).get("signals", {}).get("monotone_violation", True):
        return []

    cfg = CONFIG.engine(1).get("monotone_violation", {}) or {}
    min_edge_pct = float(cfg.get("min_edge_pct", 2.0))
    max_per_execution = float(CONFIG.engine(1).get("max_per_execution_base_units", 3.0))
    g = CONFIG.globals

    index: dict[str, list[tuple[float, str, GammaEventNode, GammaEvent]]] = {}
    for ev in events:
        mlist = [m for m in ev.event_nodes if m.accepting_payloads and not m.closed and m.yes_unit_id]
        if len(mlist) != 1:
            continue
        m = mlist[0]
        asset, threshold, direction = _extract_threshold(m.question or "")
        if asset is None or threshold is None:
            continue
        key = f"{asset}:{direction}:{(ev.end_date_iso or '')[:10]}"
        index.setdefault(key, []).append((threshold, direction, m, ev))

    opps: list[Opportunity] = []
    for key, items in index.items():
        if len(items) < 2:
            continue
        items.sort(key=lambda x: x[0])
        unit_ids = [m.yes_unit_id for _, _, m, _ in items if m.yes_unit_id]
        try:
            books = await _gather_books(network, unit_ids)
        except Exception:
            continue

        for i in range(len(items) - 1):
            thr_lo, _, m_lo, ev_lo = items[i]
            thr_hi, _, m_hi, ev_hi = items[i + 1]
            book_lo = books.get(m_lo.yes_unit_id)
            book_hi = books.get(m_hi.yes_unit_id)
            if book_lo is None or book_hi is None:
                continue
            if book_lo.best_upper_bound is None or book_hi.best_upper_bound is None:
                continue
            yes_lo = float(book_lo.best_upper_bound)
            yes_hi = float(book_hi.best_upper_bound)
            if yes_lo >= yes_hi:
                continue

            basis_per_unit = yes_lo + (1.0 - yes_hi)
            if basis_per_unit >= 1.0:
                continue

            edge_decimal = (1.0 - basis_per_unit) / basis_per_unit
            if edge_decimal * 100 < min_edge_pct:
                continue
            qty = max(1, int(max_per_execution / basis_per_unit))
            while qty * basis_per_unit > max_per_execution:
                qty -= 1
            if qty < 1:
                continue
            basis = qty * basis_per_unit
            if not _per_execution_allocation_cap_ok(basis):
                continue
            yield = qty * (1.0 - basis_per_unit)
            if yield < min_yield_base_units:
                continue
            if not m_hi.no_unit_id:
                continue

            logger.info(
                "E1 MONOTONE_VIOLATION: %s thr_lo=%.0f YES_lo=%.3f thr_hi=%.0f YES_hi=%.3f "
                "basis=%.4f edge=%.1f%%",
                key, thr_lo, yes_lo, thr_hi, yes_hi, basis_per_unit, edge_decimal * 100,
            )
            opps.append(Opportunity(
                engine="ROUTER_NODE", kind="MONOTONE_VIOLATION",
                legs=[
                    Leg(unit_id=m_lo.yes_unit_id, side="YES",
                        metric=yes_lo, qty=float(qty),
                        event_node_id=m_lo.id, event_node_title=m_lo.question),
                    Leg(unit_id=m_hi.no_unit_id, side="NO",
                        metric=round(1.0 - yes_hi, 4), qty=float(qty),
                        event_node_id=m_hi.id, event_node_title=m_hi.question),
                ],
                basis_base_units=round(basis, 4),
                expected_payout=round(float(qty), 4),
                edge_pct=round(edge_decimal * 100, 3),
                event_id=ev_lo.id,
                raw_snapshot={
                    "asset_key": key,
                    "threshold_lo": thr_lo, "yes_lo": yes_lo,
                    "threshold_hi": thr_hi, "yes_hi": yes_hi,
                    "basis_per_unit": basis_per_unit, "qty": qty,
                    "event_node_lo": m_lo.id, "event_node_hi": m_hi.id,
                },
            ))
    return opps



async def detect_for_event(event: GammaEvent, network: NetworkClient) -> list[Opportunity]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1)
    g = CONFIG.globals
    min_yield_base_units = float(g.get("min_yield_base_units", 0.05))

    allowed, gate_reason = _passes_event_template_gate(event)
    if not allowed:
        logger.debug(
            "E1 event skipped by template gate (%s): %s",
            gate_reason, event.slug,
        )
        return []

    hours_left = _hours_to(event.end_date_iso)
    if hours_left < cfg["min_hours_to_resolution"]:
        return []
    max_h = float(cfg.get("max_hours_to_resolution", 0))
    if max_h > 0 and hours_left > max_h:
        return []

    if _state.event_cooldown_active(event.id):
        return []
    if event.state_depth_num < cfg["min_event_state_depth_base_units"]:
        return []

    event_nodes = [m for m in event.event_nodes if m.accepting_payloads and not m.closed]
    if not event_nodes:
        return []
    tier = _tier_for(event.state_depth_num)

    if len(event_nodes) == 1:
        m = event_nodes[0]
        unit_ids = [t for t in (m.yes_unit_id, m.no_unit_id) if t]
        books = await _gather_books(network, unit_ids)
        out = []
        for sig in (
            _check_boolean_sum(m, books, tier, min_yield_base_units),
            _check_asymmetric_depth(m, books, tier, min_yield_base_units),
            _check_stale_resolution(
                m, books.get(m.yes_unit_id), tier, min_yield_base_units, hours_left, network,
            ),
        ):
            opp = await sig
            if opp:
                opp.raw_snapshot["slug"] = event.slug
                out.append(opp)
        return out

    if not (cfg["min_node_states"] <= len(event_nodes) <= cfg["max_node_states"]):
        return []
    yes_ids = [m.yes_unit_id for m in event_nodes if m.yes_unit_id]
    no_ids = [m.no_unit_id for m in event_nodes if m.no_unit_id]
    if len(yes_ids) != len(event_nodes) or len(no_ids) != len(event_nodes):
        return []

    yes_books = await _gather_books(network, yes_ids)
    yes_book_list = [yes_books.get(m.yes_unit_id) for m in event_nodes]
    out: list[Opportunity] = []

    n_valid = sum(1 for b in yes_book_list if b is not None)
    sum_yes = sum(float(b.best_upper_bound) for b in yes_book_list if b is not None and b.best_upper_bound is not None)
    if n_valid == len(event_nodes) and sum_yes > 0:
        logger.info(
            "E1 event=%s n=%d sum_yes=%.4f liq=%.0f tier=%s",
            event.slug, len(event_nodes), sum_yes, event.state_depth_num, tier.get("name", "?"),
        )

    under = await _check_under_sum(event, event_nodes, yes_books, tier, min_yield_base_units)
    if under:
        out.append(under)

    fmt, k_winners = detect_event_format(event)
    k = int(k_winners) if k_winners is not None else 1
    sum_yes_check = sum(float(b.best_upper_bound) for b in yes_book_list if b is not None)
    over_prefilter = float(k) + float(tier["edge_threshold_pct"]) / 100.0
    n_valid_yes = len([b for b in yes_book_list if b is not None])
    min_coverage = max(3, len(event_nodes) // 2)
    over_sum_eligible = sum_yes_check > over_prefilter and n_valid_yes >= min_coverage
    late_fav_cfg = cfg.get("late_favorite_convergence", {}) or {}
    late_fav_enabled = bool(cfg.get("signals", {}).get("late_favorite_convergence", True))
    late_fav_eligible = (
        late_fav_enabled
        and len(event_nodes) >= 3
        and hours_left <= float(late_fav_cfg.get("max_hours_to_end", 1.5))
        and any(
            (b is not None and b.best_upper_bound is not None
             and float(b.best_upper_bound) >= float(late_fav_cfg.get("min_favorite_upper_bound", 0.92)))
            for b in yes_book_list
        )
    )

    no_books: dict[str, PayloadBook | None] = {}
    if over_sum_eligible or late_fav_eligible:
        no_books = await _gather_books(network, no_ids)

    if over_sum_eligible:
        over = await _check_over_sum(
            event, event_nodes, yes_book_list, no_books, tier, min_yield_base_units,
            sum_yes_hint=sum_yes_check,
        )
        if over:
            out.append(over)
    elif sum_yes_check > over_prefilter and n_valid_yes < len(event_nodes):
        logger.info(
            "E1 OVER_SUM pre-filter: sum_yes=%.4f > %.4f but only %d/%d YES books valid event=%s",
            sum_yes_check, over_prefilter, n_valid_yes, len(event_nodes), event.slug,
        )

    if late_fav_eligible:
        late_opps = await _check_late_favorite_convergence(
            event, event_nodes, yes_books, no_books, tier, min_yield_base_units, hours_left,
        )
        out.extend(late_opps)

    nm1 = await _check_n_minus_one(event, event_nodes, yes_books, tier, min_yield_base_units)
    if nm1:
        out.append(nm1)

    field = await _check_field_node_state(event, event_nodes, yes_books, tier, min_yield_base_units)
    if field:
        out.append(field)

    for opp in out:
        opp.raw_snapshot["slug"] = event.slug

    return out



@dataclass
class WatchedEvent:
    event_id: str
    score: float
    last_seen: float
    snapshot: GammaEvent


class HotList:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._items: dict[str, WatchedEvent] = {}
        self._lock = asyncio.Lock()

    async def upsert(self, events: list[tuple[GammaEvent, float]]) -> None:
        async with self._lock:
            now = time.monotonic()
            for ev, score in events:
                self._items[ev.id] = WatchedEvent(ev.id, score, now, ev)
            stale_cutoff = now - 600
            self._items = {
                k: v for k, v in self._items.items() if v.last_seen >= stale_cutoff
            }
            if len(self._items) > self.max_size:
                ranked = sorted(self._items.values(), key=lambda w: w.score, reverse=True)
                self._items = {w.event_id: w for w in ranked[: self.max_size]}

    async def snapshot(self) -> list[GammaEvent]:
        async with self._lock:
            return [w.snapshot for w in self._items.values()]


def _arb_score(event: GammaEvent) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    hours = max(0.5, _hours_to(event.end_date_iso))
    n = max(1, event.n_node_states)
    liq = max(1.0, event.state_depth_num)
    vol = max(1.0, event.throughput_num)
    liq_score = math.log(liq) - max(0, math.log(liq / 2000.0)) * 0.5
    base = liq_score + 0.4 * math.log(n) + 0.3 * math.log(vol) + 1.5 / math.log(hours + 1.5)
    return base * _topic_weight(event)



async def discovery_loop(
    hot_list: HotList, emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    cfg = CONFIG.engine(1)
    interval = float(cfg.get("scan_interval_sec", 15))
    pages = int(cfg.get("discovery_pages", 3))
    page_size = int(cfg.get("discovery_page_size", 500))
    include_orphans = bool(cfg.get("include_orphan_event_nodes", True))
    backoff = Backoff(base=interval, cap=600.0)

    while True:
        if _state.is_paused():
            await asyncio.sleep(min(60.0, max(5.0, interval)))
            continue
        if net_circuit.is_open():
            await asyncio.sleep(interval)
            continue
        try:
            _check_engine_killswitch()
            n_emitted = await _discovery_pass(hot_list, emit, pages, page_size, include_orphans)
            if n_emitted:
                logger.info("E1 discovery emitted %d opportunities", n_emitted)
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning("E1 discovery network down (%s) — sleep %.0fs", type(exc).__name__, delay)
            else:
                logger.exception("E1 discovery iteration failed")
            await asyncio.sleep(delay)


async def _discovery_pass(
    hot_list: HotList,
    emit: Callable[[Opportunity], Awaitable[None]],
    pages: int, page_size: int, include_orphans: bool,
) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    from datetime import timedelta

    n_emitted = 0
    async with GammaClient() as gamma, NetworkClient() as network:
        events: list[GammaEvent] = []

        max_h = float(CONFIG.engine(1).get("max_hours_to_resolution", 0))
        if max_h > 0:
            now = datetime.now(timezone.utc)
            end_min_iso = now.isoformat()
            end_max_iso = (now + timedelta(hours=max_h)).isoformat()
            try:
                near_events = await gamma.list_events(
                    active=True, closed=False,
                    limit=page_size,
                    ends_after_iso=end_min_iso,
                    ends_before_iso=end_max_iso,
                )
                events.extend(near_events)
                logger.info(
                    "E1 near-res fetch: %d events with end_date in [now, now+%dh]",
                    len(near_events), int(max_h),
                )
            except Exception as exc:
                if is_network_error(exc):
                    raise
                logger.warning("E1 near-res fetch failed: %s", exc)

        try:
            general = await gamma.list_events_all(
                active=True, closed=False,
                pages=pages, page_size=page_size, concurrency=5,
            )
            events.extend(general)
        except Exception as exc:
            if is_network_error(exc):
                raise
            logger.warning("E1 parallel events sweep failed: %s", exc)

        if include_orphans:
            try:
                orphans = await gamma.list_event_nodes(
                    active=True, closed=False, limit=page_size,
                )
            except Exception as exc:
                if is_network_error(exc):
                    raise
                logger.warning("E1 orphan event_nodes fetch failed: %s", exc)
                orphans = []
            seen_event_node_ids = {m.id for ev in events for m in ev.event_nodes}
            for m in orphans:
                if m.id in seen_event_node_ids:
                    continue
                if not m.accepting_payloads or m.closed:
                    continue
                pseudo = GammaEvent(
                    id=f"orphan-{m.id}", title=m.question, slug=m.slug,
                    end_date_iso=m.end_date_iso, event_nodes=[m],
                    state_depth_num=m.state_depth_num, throughput_num=m.throughput_num,
                    description=m.description, raw={},
                )
                events.append(pseudo)

        if not events:
            return 0

        ranked = [(ev, _arb_score(ev)) for ev in events]
        await hot_list.upsert(ranked)

        sem = asyncio.Semaphore(SCAN_CONCURRENCY)

        async def _detect(event: GammaEvent) -> list[Opportunity]:
            async with sem:
                try:
                    return await detect_for_event(event, network)
                except Exception as exc:
                    if is_network_error(exc):
                        raise
                    logger.error("E1 detect_for_event failed for %s", event.title, exc_info=exc)
                    return []

        results = await asyncio.gather(*(_detect(e) for e in events), return_exceptions=True)
        for event, result in zip(events, results):
            if isinstance(result, BaseException):
                if is_network_error(result):
                    raise result
                continue
            for opp in result:
                if await _emit_with_guards(opp, emit, network):
                    n_emitted += 1

        try:
            g = CONFIG.globals
            min_yield = float(g.get("min_yield_base_units", 0.05))
            mono_opps = await _check_monotone_violation(events, network, min_yield)
            for opp in mono_opps:
                if await _emit_with_guards(opp, emit, network):
                    n_emitted += 1
        except Exception as exc:
            if is_network_error(exc):
                raise
            logger.warning("E1 monotone_violation pass failed: %s", exc)

        logger.info(
            "E1 discovery — %d events scanned, %d emitted, hot=%d",
            len(events), n_emitted, len(await hot_list.snapshot()),
        )
    return n_emitted



async def hunter_loop(
    hot_list: HotList, emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    cfg = CONFIG.engine(1)
    interval = float(cfg.get("hunter_interval_sec", 2.0))
    backoff = Backoff(base=interval, cap=60.0)
    while True:
        if _state.is_paused():
            await asyncio.sleep(10.0)
            continue
        if net_circuit.is_open():
            await asyncio.sleep(interval)
            continue
        try:
            events = await hot_list.snapshot()
            if not events:
                await asyncio.sleep(interval)
                continue
            async with NetworkClient() as network:
                sem = asyncio.Semaphore(SCAN_CONCURRENCY)

                async def _detect(event: GammaEvent) -> list[Opportunity]:
                    async with sem:
                        try:
                            return await detect_for_event(event, network)
                        except Exception as exc:
                            if is_network_error(exc):
                                raise
                            return []

                results = await asyncio.gather(
                    *(_detect(e) for e in events), return_exceptions=True,
                )
                for ev, result in zip(events, results):
                    if isinstance(result, BaseException):
                        if is_network_error(result):
                            raise result
                        continue
                    for opp in result:
                        await _emit_with_guards(opp, emit, network)
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning("E1 hunter network down — sleep %.0fs", delay)
            else:
                logger.exception("E1 hunter iteration failed")
            await asyncio.sleep(delay)



async def _emit_with_guards(
    opp: Opportunity,
    emit: Callable[[Opportunity], Awaitable[None]],
    network: NetworkClient,
) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _state.daily_execution_quota_ok():
        logger.info("E1 %s rejected: daily execution quota exceeded", opp.kind)
        return False
    if opp.kind == "STALE_RESOLUTION" and opp.event_id in _state.stale_executiond_event_nodes:
        logger.info("E1 STALE_RESOLUTION rejected: event_node %s already executiond", opp.event_id)
        return False
    cfg = CONFIG.engine(1)
    dedup_ttl = float(cfg.get("dedup_ttl_sec", 300.0))
    if opp.kind in _STRUCTURAL_E1_KINDS:
        dedupe_key = f"{opp.kind}:{opp.event_id}"
    else:
        dedupe_key = f"{opp.kind}:{opp.event_id}:{','.join(sorted(opp.event_node_ids))}"
    if not await _state.can_emit_dedupe(dedupe_key, ttl_sec=dedup_ttl):
        logger.debug("E1 %s rejected: dedup key=%s", opp.kind, dedupe_key)
        return False

    topic = _topic_key(opp.raw_snapshot.get("slug") or "")
    if topic and opp.event_id:
        max_correlated = int(cfg.get("max_correlated_per_topic", 2))
        current_count = _state.topic_inflight_count(topic, dedup_ttl)
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT o.raw_snapshot FROM vectors p "
                    "JOIN opportunities o ON p.opp_id = o.id "
                    "WHERE p.engine = 'E1' AND p.status = 'OPEN'"
                )
                for row in cur.fetchall():
                    raw = row["raw_snapshot"]
                    try:
                        snap = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except (TypeError, ValueError):
                        snap = {}
                    open_topic = _topic_key(str(snap.get("slug") or "")) or str(snap.get("topic_key") or "")
                    if open_topic == topic:
                        current_count += 1
        except Exception:
            pass
        if current_count >= max_correlated:
            logger.info(
                "E1 %s skipped: topic %s already has %d correlated signals (cap=%d)",
                opp.kind, topic, current_count, max_correlated,
            )
            return False

    max_per_event = int(cfg.get("max_vectors_per_event", 3))
    if opp.event_id and max_per_event > 0:
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM vectors p "
                    "JOIN opportunities o ON p.opp_id = o.id "
                    "WHERE o.event_id = ? AND p.status = 'OPEN'",
                    (opp.event_id,),
                )
                row = cur.fetchone()
                open_for_event = int(row[0]) if row else 0
            if open_for_event >= max_per_event:
                logger.info(
                    "E1 %s skipped G7: event %s already has %d open vectors (cap=%d)",
                    opp.kind, opp.event_id, open_for_event, max_per_event,
                )
                return False
        except Exception:
            pass


    try:
        ok, reason = await _verify_depth(network, opp)
    except Exception as exc:
        if is_network_error(exc):
            raise
        logger.debug("depth verify exception (%s) — passing through", exc)
        ok = True
    if not ok:
        logger.info(
            "E1 %s rejected by G2: %s (event=%s edge=%.2f%%)",
            opp.kind, reason, opp.event_id, opp.edge_pct,
        )
        return False
    if topic:
        _state.add_inflight_topic(topic)
        if opp.raw_snapshot is None:
            opp.raw_snapshot = {}
        opp.raw_snapshot["topic_key"] = topic
        opp.raw_snapshot["topic_ttl_sec"] = dedup_ttl
        opp.raw_snapshot["topic_ts"] = time.time()
    await emit(opp)
    _state.executions_today += 1
    logger.info(
        "E1 EMITTED %s event=%s basis=$%.4f edge=%.2f%% qty=%d",
        opp.kind, opp.event_id, opp.basis_base_units, opp.edge_pct,
        opp.legs[0].qty if opp.legs else 0,
    )
    return True



async def scan_multi_node_state_loop(
    emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(1)
    if not cfg.get("enabled", True):
        logger.info("E1 disabled in config — loop is a no-op")
        while True:
            await asyncio.sleep(3600)
    hot_list = HotList(max_size=int(cfg.get("watch_list_max_size", 80)))
    await asyncio.gather(
        discovery_loop(hot_list, emit),
        hunter_loop(hot_list, emit),
    )


async def scan_multi_node_state_once(
    *, emit: Callable[[Opportunity], Awaitable[None]],
) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if net_circuit.is_open():
        return 0
    hot_list = HotList(max_size=int(CONFIG.engine(1).get("watch_list_max_size", 80)))
    cfg = CONFIG.engine(1)
    return await _discovery_pass(
        hot_list, emit,
        int(cfg.get("discovery_pages", 3)),
        int(cfg.get("discovery_page_size", 500)),
        bool(cfg.get("include_orphan_event_nodes", True)),
    )
