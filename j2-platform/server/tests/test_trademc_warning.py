"""Regression tests for the TradeMC 'created-in-last-7-days' soft-warning
guard used when a user assigns a PMX trade to a trade number.

Contract (mirrored from server._pmx_trade_number_trademc_warning):
  - Empty trade number -> no warning
  - Non-integer ref (e.g. 'KA2604') -> no warning (client-initial)
  - Excluded symbols (XAGUSD/XPTUSD/XPDUSD) -> no warning
  - Integer ref with no matching TradeMC booking -> WARNING
  - Integer ref whose latest TradeMC date_created is >7 days old -> WARNING
  - Integer ref with a TradeMC date_created within 7 days -> no warning
  - Ref-number normalization (spaces/dashes/slashes) matches TradeMC row

Endpoint flow (mirrored from PUT /api/pmx/trades/<id>/trade-number):
  - No warning -> assignment commits, response ok
  - Warning + no override -> 409, requires_confirmation, NOTHING written
  - Warning + override_validation=true -> assignment commits

Run:  python -m pytest tests/test_trademc_warning.py
      python tests/test_trademc_warning.py
"""

import os
import re
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone


# ----------------------------------------------------------------------
# Validator re-implemented inline. Must stay in sync with
# server._pmx_trade_number_trademc_warning.
# ----------------------------------------------------------------------

EXCLUDED_SYMBOLS = {"XAGUSD", "XPTUSD", "XPDUSD"}


def _normalize_symbol(val):
    return re.sub(r"[^A-Z0-9]", "", str(val or "").upper())


def _normalize_ref(val):
    return re.sub(r"[\s\-\/]", "", str(val or "").strip().upper())


def trademc_warning(conn, trade_number, trade_symbol, days=7, now_utc=None):
    """Returns (should_warn: bool, warning_msg: str)."""
    normalized = _normalize_ref(trade_number)
    if not normalized:
        return False, ""
    if not re.fullmatch(r"\d+", normalized):
        return False, ""
    if _normalize_symbol(trade_symbol) in EXCLUDED_SYMBOLS:
        return False, ""

    rows = conn.execute("""
        SELECT ref_number, date_created FROM trademc_trades
        WHERE UPPER(REPLACE(REPLACE(REPLACE(TRIM(COALESCE(ref_number, '')), ' ', ''), '-', ''), '/', '')) = ?
    """, (normalized,)).fetchall()

    now = now_utc or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, int(days)))
    latest = None
    for _, dc in rows:
        if not dc or not str(dc).strip():
            continue
        try:
            parsed = datetime.fromisoformat(str(dc).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if latest is None or parsed > latest:
            latest = parsed

    if latest is None or latest < cutoff:
        return True, f"Trade number '{normalized}' has not been created in TradeMC in the past {int(days)} days — assign anyway?"
    return False, ""


# ----------------------------------------------------------------------
# Minimal PMX trades table + UPSERT + manual override (so we can assert
# that a 409-warning response does NOT mutate DB state, and an
# override=true request DOES.)
# ----------------------------------------------------------------------

PMX_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    doc_number TEXT UNIQUE,
    order_id TEXT,
    order_id_source TEXT
)
"""

TRADEMC_SCHEMA = """
CREATE TABLE IF NOT EXISTS trademc_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ref_number TEXT,
    date_created TEXT
)
"""


def simulate_pmx_assign(pmx_conn, tmc_conn, *, trade_id, trade_number,
                        override_validation=False, now_utc=None):
    """Simulate the PMX endpoint handler.

    Returns a response dict mirroring the Flask endpoint:
      - {status: 200, body: {...}}  on success
      - {status: 409, body: {requires_confirmation: True, warning: ...}}  on soft warning
      - {status: 404, body: {...}}  if trade_id missing
    Crucially: the 409 path does NOT mutate the PMX row.
    """
    row = pmx_conn.execute(
        "SELECT symbol FROM trades WHERE id = ?", (trade_id,)
    ).fetchone()
    if not row:
        return {"status": 404, "body": {"ok": False, "error": "not found"}}

    trade_symbol = row[0] or ""

    if trade_number and not override_validation:
        should_warn, msg = trademc_warning(tmc_conn, trade_number, trade_symbol,
                                           now_utc=now_utc)
        if should_warn:
            return {
                "status": 409,
                "body": {"ok": False, "requires_confirmation": True, "warning": msg},
            }

    # Commit the manual override (mirrors update_pmx_trade_order_id).
    val = _normalize_ref(trade_number) or None
    if val:
        pmx_conn.execute(
            "UPDATE trades SET order_id = ?, order_id_source = 'manual' WHERE id = ?",
            (val, trade_id),
        )
    else:
        pmx_conn.execute(
            "UPDATE trades SET order_id = NULL, order_id_source = NULL WHERE id = ?",
            (trade_id,),
        )
    pmx_conn.commit()
    return {"status": 200, "body": {"ok": True, "trade_id": trade_id, "trade_number": val}}


# ----------------------------------------------------------------------
# Test harness
# ----------------------------------------------------------------------

class TradeMCWarningGateRules(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(TRADEMC_SCHEMA)

    def _insert(self, ref_number, created_days_ago):
        ts = (datetime.now(timezone.utc) - timedelta(days=created_days_ago)).isoformat()
        self.conn.execute(
            "INSERT INTO trademc_trades (ref_number, date_created) VALUES (?, ?)",
            (ref_number, ts),
        )
        self.conn.commit()

    def test_empty_ref_no_warning(self):
        self.assertEqual(trademc_warning(self.conn, "", "XAUUSD")[0], False)

    def test_non_integer_ref_no_warning(self):
        # Client-initial tickets (KA2604, ADJ-001, Jos-070) have no TradeMC booking.
        for ref in ("KA2604", "ADJ-001", "JOS-070"):
            self.assertEqual(trademc_warning(self.conn, ref, "XAUUSD")[0], False, ref)

    def test_excluded_symbol_no_warning(self):
        for sym in ("XAGUSD", "XPTUSD", "XPDUSD", "XPT/USD"):
            self.assertEqual(trademc_warning(self.conn, "12345", sym)[0], False, sym)

    def test_integer_ref_no_match_warns(self):
        should_warn, msg = trademc_warning(self.conn, "99999", "XAUUSD")
        self.assertTrue(should_warn)
        self.assertIn("99999", msg)
        self.assertIn("7 days", msg)

    def test_integer_ref_booking_older_than_7_days_warns(self):
        self._insert("12345", created_days_ago=14)
        should_warn, _ = trademc_warning(self.conn, "12345", "XAUUSD")
        self.assertTrue(should_warn)

    def test_integer_ref_booking_within_7_days_no_warning(self):
        self._insert("12345", created_days_ago=2)
        should_warn, _ = trademc_warning(self.conn, "12345", "XAUUSD")
        self.assertFalse(should_warn)

    def test_uses_most_recent_when_multiple_bookings_exist(self):
        # Older booking outside window, newer one inside → no warning.
        self._insert("12345", created_days_ago=30)
        self._insert("12345", created_days_ago=1)
        should_warn, _ = trademc_warning(self.conn, "12345", "XAUUSD")
        self.assertFalse(should_warn)

    def test_ref_number_normalization_matches_variants(self):
        # A ref stored as "1 2-3/45" in TradeMC should still match "12345".
        self._insert("1 2-3/45", created_days_ago=1)
        should_warn, _ = trademc_warning(self.conn, "12345", "XAUUSD")
        self.assertFalse(should_warn)


class TradeMCWarningEndpointFlow(unittest.TestCase):
    """Verify the allocate / reassign endpoint contract on BOTH the
    unallocated table (order_id initially NULL) and the allocated table
    (order_id initially populated from comments)."""

    def setUp(self):
        self.pmx = sqlite3.connect(":memory:")
        self.pmx.execute(PMX_SCHEMA)
        self.tmc = sqlite3.connect(":memory:")
        self.tmc.execute(TRADEMC_SCHEMA)

        # Unallocated row (order_id NULL).
        self.pmx.execute(
            "INSERT INTO trades (id, symbol, doc_number, order_id, order_id_source) VALUES (1, 'XAUUSD', 'FNC/2026/000001', NULL, NULL)"
        )
        # Allocated row (order_id populated from comments).
        self.pmx.execute(
            "INSERT INTO trades (id, symbol, doc_number, order_id, order_id_source) VALUES (2, 'XAUUSD', 'FNC/2026/000002', 'KA2604', 'comment')"
        )
        self.pmx.commit()

        # Seed one TradeMC booking that IS within 7 days.
        ts_recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        self.tmc.execute(
            "INSERT INTO trademc_trades (ref_number, date_created) VALUES ('70001', ?)",
            (ts_recent,),
        )
        self.tmc.commit()

    def _row(self, trade_id):
        return self.pmx.execute(
            "SELECT order_id, order_id_source FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    # -- Unallocated path --------------------------------------------------

    def test_unallocated_assign_known_ref_commits(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=1, trade_number="70001")
        self.assertEqual(res["status"], 200)
        self.assertEqual(self._row(1), ("70001", "manual"))

    def test_unallocated_assign_unknown_ref_returns_409(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=1, trade_number="99999")
        self.assertEqual(res["status"], 409)
        self.assertTrue(res["body"]["requires_confirmation"])
        # Critical: the DB row is UNCHANGED.
        self.assertEqual(self._row(1), (None, None))

    def test_unallocated_assign_unknown_ref_with_override_commits(self):
        res = simulate_pmx_assign(
            self.pmx, self.tmc, trade_id=1, trade_number="99999",
            override_validation=True,
        )
        self.assertEqual(res["status"], 200)
        self.assertEqual(self._row(1), ("99999", "manual"))

    # -- Allocated (reassignment) path -------------------------------------

    def test_allocated_reassign_to_unknown_ref_returns_409(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=2, trade_number="99999")
        self.assertEqual(res["status"], 409)
        self.assertTrue(res["body"]["requires_confirmation"])
        # Original comment-derived allocation preserved.
        self.assertEqual(self._row(2), ("KA2604", "comment"))

    def test_allocated_reassign_to_unknown_ref_with_override_commits(self):
        res = simulate_pmx_assign(
            self.pmx, self.tmc, trade_id=2, trade_number="99999",
            override_validation=True,
        )
        self.assertEqual(res["status"], 200)
        self.assertEqual(self._row(2), ("99999", "manual"))

    def test_allocated_reassign_to_known_ref_commits_without_warning(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=2, trade_number="70001")
        self.assertEqual(res["status"], 200)
        self.assertEqual(self._row(2), ("70001", "manual"))

    # -- Non-integer refs (KA-style) should NEVER warn ---------------------

    def test_non_integer_ref_assigns_without_warning_unallocated(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=1, trade_number="KA9999")
        self.assertEqual(res["status"], 200)
        self.assertEqual(self._row(1), ("KA9999", "manual"))

    def test_non_integer_ref_assigns_without_warning_allocated(self):
        res = simulate_pmx_assign(self.pmx, self.tmc, trade_id=2, trade_number="ADJ-001")
        self.assertEqual(res["status"], 200)
        # Normalization strips the dash.
        self.assertEqual(self._row(2), ("ADJ001", "manual"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
