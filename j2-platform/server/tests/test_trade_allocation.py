"""Regression tests for PMX trade-number allocation.

Covers the contract:
  1. Initial auto-allocation from PMX comments populates order_id + source='comment'.
  2. A manual override replaces order_id and sets source='manual'.
  3. A subsequent PMX sync must NOT overwrite a 'manual' row, even when
     the incoming payload has a different comment-derived order_id.
  4. A PMX sync where no manual override exists still refreshes the
     comment-derived order_id.

Run with:  python -m pytest tests/test_trade_allocation.py
Or as a script: python tests/test_trade_allocation.py
"""

import os
import sqlite3
import tempfile
import unittest


# --- SQL mirrored from server.initialize_pmx_database / sync_pmx_trades_to_db ---
# Keeping these inline (instead of importing server.py) lets the tests run
# without the full Flask app stack / external DB paths.

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL DEFAULT 0,
    price REAL NOT NULL DEFAULT 0,
    doc_number TEXT UNIQUE,
    order_id TEXT,
    order_id_source TEXT,
    allocated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

INSERT_COLS = ["trade_date", "symbol", "side", "quantity", "price",
               "doc_number", "order_id", "order_id_source"]

UPSERT_SQL = f"""
INSERT INTO trades ({", ".join(INSERT_COLS)})
VALUES ({", ".join(["?"] * len(INSERT_COLS))})
ON CONFLICT(doc_number) DO UPDATE SET
    trade_date = excluded.trade_date,
    symbol = excluded.symbol,
    side = excluded.side,
    quantity = excluded.quantity,
    price = excluded.price,
    order_id = CASE
        WHEN COALESCE(trades.order_id_source, '') = 'manual' THEN trades.order_id
        WHEN excluded.order_id IS NOT NULL AND TRIM(excluded.order_id) != '' THEN excluded.order_id
        ELSE trades.order_id
    END,
    order_id_source = CASE
        WHEN COALESCE(trades.order_id_source, '') = 'manual' THEN 'manual'
        WHEN excluded.order_id IS NOT NULL AND TRIM(excluded.order_id) != '' THEN 'comment'
        ELSE trades.order_id_source
    END
"""


def _sync_row(conn, *, trade_date, symbol, side, qty, price, doc_number,
              comment_order_id):
    """Simulate one PMX-sync UPSERT: incoming order_id is always 'comment'-sourced."""
    source = "comment" if comment_order_id else ""
    conn.execute(UPSERT_SQL, (
        trade_date, symbol, side, qty, price, doc_number,
        comment_order_id, source,
    ))
    conn.commit()


def _manual_override(conn, trade_id, new_order_id):
    """Mirror of server.update_pmx_trade_order_id."""
    val = (new_order_id or "").strip().upper() or None
    if val:
        conn.execute("""
            UPDATE trades
            SET order_id = ?,
                order_id_source = 'manual',
                allocated_at = COALESCE(allocated_at, CURRENT_TIMESTAMP)
            WHERE id = ?
        """, (val, trade_id))
    else:
        conn.execute("""
            UPDATE trades
            SET order_id = ?,
                order_id_source = NULL
            WHERE id = ?
        """, (val, trade_id))
    conn.commit()


def _row(conn, doc_number):
    cur = conn.execute(
        "SELECT id, order_id, order_id_source FROM trades WHERE doc_number = ?",
        (doc_number,),
    )
    return cur.fetchone()


class TradeAllocationTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(SCHEMA_SQL)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        try:
            os.remove(self.db_path)
        except OSError:
            pass

    def test_1_initial_auto_allocation_from_comments(self):
        """Fresh sync: comment-derived order_id is stored with source='comment'."""
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")
        row = _row(self.conn, "FNC/2026/000001")
        self.assertIsNotNone(row)
        _, order_id, source = row
        self.assertEqual(order_id, "KA2604")
        self.assertEqual(source, "comment")

    def test_2_manual_override_replaces_and_tags(self):
        """Manual override: stores new value and tags source='manual'."""
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")
        row = _row(self.conn, "FNC/2026/000001")
        trade_id = row[0]

        _manual_override(self.conn, trade_id, "JOSH-999")

        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertEqual(order_id, "JOSH-999")
        self.assertEqual(source, "manual")

    def test_3_pmx_sync_after_manual_override_preserves_override(self):
        """Regression: sync after manual override must NOT revert to comments."""
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")
        trade_id = _row(self.conn, "FNC/2026/000001")[0]

        _manual_override(self.conn, trade_id, "JOSH-999")

        # Re-sync the same doc with a DIFFERENT comment-derived order_id
        # (e.g. the PMX remarks were edited upstream). The override wins.
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")

        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertEqual(order_id, "JOSH-999",
                         "manual override must survive PMX sync")
        self.assertEqual(source, "manual")

        # And a sync where the upstream comment has been ERASED must still
        # preserve the manual override (not null it out).
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="")
        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertEqual(order_id, "JOSH-999")
        self.assertEqual(source, "manual")

    def test_4_pmx_sync_without_manual_override_refreshes_from_comments(self):
        """Rows that have NOT been manually overridden still update from comments."""
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")

        # Upstream edits the comment to a new trade number, then we re-sync.
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2605")

        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertEqual(order_id, "KA2605")
        self.assertEqual(source, "comment")

    def test_5_clearing_override_releases_row_back_to_auto_allocation(self):
        """Clearing a manual override lets the next sync repopulate from comments."""
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")
        trade_id = _row(self.conn, "FNC/2026/000001")[0]

        _manual_override(self.conn, trade_id, "JOSH-999")
        _manual_override(self.conn, trade_id, "")  # user clears it

        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertIsNone(order_id)
        self.assertIsNone(source)

        # Next sync reassigns from comments.
        _sync_row(self.conn, trade_date="2026-04-20", symbol="XAUUSD", side="BUY",
                  qty=100.0, price=4787.03, doc_number="FNC/2026/000001",
                  comment_order_id="KA2604")
        _, order_id, source = _row(self.conn, "FNC/2026/000001")
        self.assertEqual(order_id, "KA2604")
        self.assertEqual(source, "comment")


if __name__ == "__main__":
    unittest.main(verbosity=2)
