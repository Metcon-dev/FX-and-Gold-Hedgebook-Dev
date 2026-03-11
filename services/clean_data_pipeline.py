"""Integrity-first data warehouse pipeline for J2 platform datasets."""
import hashlib
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEAN_DB_PATH = os.path.join(PROJECT_ROOT, "j2_platform_clean.db")


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _get_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def initialize_clean_pipeline_db(clean_db_path: Optional[str] = None) -> None:
    """Create clean warehouse tables used by the integrity pipeline."""
    db_path = clean_db_path or CLEAN_DB_PATH
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL,
            ledger_db_path TEXT NOT NULL,
            pmx_db_path TEXT NOT NULL,
            error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_batch_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            dataset TEXT NOT NULL,
            records_upserted INTEGER NOT NULL DEFAULT 0,
            records_seen INTEGER NOT NULL DEFAULT 0,
            dataset_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES pipeline_batches(id) ON DELETE CASCADE,
            UNIQUE(batch_id, dataset)
        )
        """
    )

    # Canonical clean tables (normalized + raw snapshot + integrity hash).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_pmx_trades (
            source_pk TEXT PRIMARY KEY,
            trade_date TEXT,
            value_date TEXT,
            symbol TEXT,
            side TEXT,
            quantity REAL,
            price REAL,
            order_id TEXT,
            fnc_number TEXT,
            last_synced TEXT,
            row_hash TEXT NOT NULL CHECK(length(row_hash)=64),
            raw_json TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_ledger_trades (
            source_pk TEXT PRIMARY KEY,
            trade_date TEXT,
            value_date TEXT,
            symbol TEXT,
            side TEXT,
            quantity REAL,
            price REAL,
            order_id TEXT,
            fnc_number TEXT,
            created_at TEXT,
            row_hash TEXT NOT NULL CHECK(length(row_hash)=64),
            raw_json TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_trademc_trades (
            source_pk INTEGER PRIMARY KEY,
            status TEXT,
            company_id INTEGER,
            weight REAL,
            ref_number TEXT,
            trade_timestamp TEXT,
            date_updated TEXT,
            last_synced TEXT,
            row_hash TEXT NOT NULL CHECK(length(row_hash)=64),
            raw_json TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_trademc_companies (
            source_pk INTEGER PRIMARY KEY,
            status TEXT,
            company_name TEXT,
            refining_rate REAL,
            date_updated TEXT,
            last_synced TEXT,
            row_hash TEXT NOT NULL CHECK(length(row_hash)=64),
            raw_json TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_trademc_weight_transactions (
            source_pk INTEGER PRIMARY KEY,
            company_id INTEGER,
            type TEXT,
            weight REAL,
            rolling_balance REAL,
            transaction_timestamp TEXT,
            date_updated TEXT,
            last_synced TEXT,
            row_hash TEXT NOT NULL CHECK(length(row_hash)=64),
            raw_json TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_pmx_trade_date ON clean_pmx_trades(trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_ledger_trade_date ON clean_ledger_trades(trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_tm_trade_ts ON clean_trademc_trades(trade_timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_tm_company_name ON clean_trademc_companies(company_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_tm_weight_ts ON clean_trademc_weight_transactions(transaction_timestamp)")

    conn.commit()
    conn.close()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _row_hash_and_json(row: Dict[str, Any]) -> Tuple[str, str]:
    normalized: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, bytes):
            normalized[key] = value.decode("utf-8", errors="replace")
        else:
            normalized[key] = value
    raw_json = json.dumps(normalized, sort_keys=True, ensure_ascii=True, default=str, separators=(",", ":"))
    digest = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    return digest, raw_json


def _load_rows(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    if not _table_exists(conn, table_name):
        return []
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    return [dict(r) for r in cur.fetchall()]


def _upsert_clean_rows(
    conn: sqlite3.Connection,
    table_name: str,
    rows: List[Tuple[Any, ...]],
    upsert_sql: str,
) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(upsert_sql, rows)
    return len(rows)


def run_clean_data_pipeline(
    ledger_db_path: str,
    pmx_db_path: str,
    clean_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build/refresh clean integrity tables from operational PMX + ledger databases.

    This pipeline preserves every source row as JSON with a deterministic hash
    while also maintaining key normalized columns for analytics.
    """
    target_db = clean_db_path or CLEAN_DB_PATH
    initialize_clean_pipeline_db(target_db)

    conn_clean = _get_conn(target_db)
    cur_clean = conn_clean.cursor()
    started_at = _utc_now_iso()
    cur_clean.execute(
        """
        INSERT INTO pipeline_batches (started_at, status, ledger_db_path, pmx_db_path)
        VALUES (?, 'running', ?, ?)
        """,
        (started_at, ledger_db_path, pmx_db_path),
    )
    batch_id = int(cur_clean.lastrowid)
    conn_clean.commit()

    metrics: Dict[str, Dict[str, Any]] = {}

    def _save_metric(dataset: str, seen: int, upserted: int, rows_hash: str) -> None:
        metrics[dataset] = {
            "records_seen": int(seen),
            "records_upserted": int(upserted),
            "dataset_hash": rows_hash,
        }

    try:
        conn_ledger = _get_conn(ledger_db_path)
        conn_pmx = _get_conn(pmx_db_path)

        # PMX trades.
        pmx_rows = _load_rows(conn_pmx, "trades")
        pmx_upserts: List[Tuple[Any, ...]] = []
        pmx_hash_feed: List[str] = []
        loaded_at = _utc_now_iso()
        for row in pmx_rows:
            row_hash, raw_json = _row_hash_and_json(row)
            source_pk = _to_text(row.get("doc_number")) or _to_text(row.get("id"))
            if not source_pk:
                continue
            pmx_hash_feed.append(row_hash)
            pmx_upserts.append(
                (
                    source_pk,
                    _to_text(row.get("trade_date")),
                    _to_text(row.get("value_date")),
                    _to_text(row.get("symbol")),
                    _to_text(row.get("side")),
                    _to_float(row.get("quantity")),
                    _to_float(row.get("price")),
                    _to_text(row.get("order_id")),
                    _to_text(row.get("fnc_number")),
                    _to_text(row.get("last_synced")),
                    row_hash,
                    raw_json,
                    loaded_at,
                )
            )
        upserted = _upsert_clean_rows(
            conn_clean,
            "clean_pmx_trades",
            pmx_upserts,
            """
            INSERT INTO clean_pmx_trades (
                source_pk, trade_date, value_date, symbol, side, quantity, price,
                order_id, fnc_number, last_synced, row_hash, raw_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_pk) DO UPDATE SET
                trade_date=excluded.trade_date,
                value_date=excluded.value_date,
                symbol=excluded.symbol,
                side=excluded.side,
                quantity=excluded.quantity,
                price=excluded.price,
                order_id=excluded.order_id,
                fnc_number=excluded.fnc_number,
                last_synced=excluded.last_synced,
                row_hash=excluded.row_hash,
                raw_json=excluded.raw_json,
                loaded_at=excluded.loaded_at
            """,
        )
        pmx_dataset_hash = hashlib.sha256("|".join(sorted(pmx_hash_feed)).encode("utf-8")).hexdigest() if pmx_hash_feed else hashlib.sha256(b"").hexdigest()
        _save_metric("pmx_trades", len(pmx_rows), upserted, pmx_dataset_hash)

        # Legacy/main ledger trades table.
        ledger_rows = _load_rows(conn_ledger, "trades")
        ledger_upserts: List[Tuple[Any, ...]] = []
        ledger_hash_feed: List[str] = []
        loaded_at = _utc_now_iso()
        for row in ledger_rows:
            row_hash, raw_json = _row_hash_and_json(row)
            source_pk = _to_text(row.get("id")) or _to_text(row.get("doc_number"))
            if not source_pk:
                continue
            ledger_hash_feed.append(row_hash)
            ledger_upserts.append(
                (
                    source_pk,
                    _to_text(row.get("trade_date")),
                    _to_text(row.get("value_date")),
                    _to_text(row.get("symbol")),
                    _to_text(row.get("side")),
                    _to_float(row.get("quantity")),
                    _to_float(row.get("price")),
                    _to_text(row.get("order_id")),
                    _to_text(row.get("fnc_number")),
                    _to_text(row.get("created_at")),
                    row_hash,
                    raw_json,
                    loaded_at,
                )
            )
        upserted = _upsert_clean_rows(
            conn_clean,
            "clean_ledger_trades",
            ledger_upserts,
            """
            INSERT INTO clean_ledger_trades (
                source_pk, trade_date, value_date, symbol, side, quantity, price,
                order_id, fnc_number, created_at, row_hash, raw_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_pk) DO UPDATE SET
                trade_date=excluded.trade_date,
                value_date=excluded.value_date,
                symbol=excluded.symbol,
                side=excluded.side,
                quantity=excluded.quantity,
                price=excluded.price,
                order_id=excluded.order_id,
                fnc_number=excluded.fnc_number,
                created_at=excluded.created_at,
                row_hash=excluded.row_hash,
                raw_json=excluded.raw_json,
                loaded_at=excluded.loaded_at
            """,
        )
        ledger_dataset_hash = hashlib.sha256("|".join(sorted(ledger_hash_feed)).encode("utf-8")).hexdigest() if ledger_hash_feed else hashlib.sha256(b"").hexdigest()
        _save_metric("ledger_trades", len(ledger_rows), upserted, ledger_dataset_hash)

        # TradeMC trades.
        tm_rows = _load_rows(conn_ledger, "trademc_trades")
        tm_upserts: List[Tuple[Any, ...]] = []
        tm_hash_feed: List[str] = []
        loaded_at = _utc_now_iso()
        for row in tm_rows:
            source_pk = row.get("id")
            if source_pk is None:
                continue
            row_hash, raw_json = _row_hash_and_json(row)
            tm_hash_feed.append(row_hash)
            tm_upserts.append(
                (
                    int(source_pk),
                    _to_text(row.get("status")),
                    int(row["company_id"]) if row.get("company_id") is not None else None,
                    _to_float(row.get("weight")),
                    _to_text(row.get("ref_number")),
                    _to_text(row.get("trade_timestamp")),
                    _to_text(row.get("date_updated")),
                    _to_text(row.get("last_synced")),
                    row_hash,
                    raw_json,
                    loaded_at,
                )
            )
        upserted = _upsert_clean_rows(
            conn_clean,
            "clean_trademc_trades",
            tm_upserts,
            """
            INSERT INTO clean_trademc_trades (
                source_pk, status, company_id, weight, ref_number, trade_timestamp,
                date_updated, last_synced, row_hash, raw_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_pk) DO UPDATE SET
                status=excluded.status,
                company_id=excluded.company_id,
                weight=excluded.weight,
                ref_number=excluded.ref_number,
                trade_timestamp=excluded.trade_timestamp,
                date_updated=excluded.date_updated,
                last_synced=excluded.last_synced,
                row_hash=excluded.row_hash,
                raw_json=excluded.raw_json,
                loaded_at=excluded.loaded_at
            """,
        )
        tm_dataset_hash = hashlib.sha256("|".join(sorted(tm_hash_feed)).encode("utf-8")).hexdigest() if tm_hash_feed else hashlib.sha256(b"").hexdigest()
        _save_metric("trademc_trades", len(tm_rows), upserted, tm_dataset_hash)

        # TradeMC companies.
        company_rows = _load_rows(conn_ledger, "trademc_companies")
        company_upserts: List[Tuple[Any, ...]] = []
        company_hash_feed: List[str] = []
        loaded_at = _utc_now_iso()
        for row in company_rows:
            source_pk = row.get("id")
            if source_pk is None:
                continue
            row_hash, raw_json = _row_hash_and_json(row)
            company_hash_feed.append(row_hash)
            company_upserts.append(
                (
                    int(source_pk),
                    _to_text(row.get("status")),
                    _to_text(row.get("company_name")),
                    _to_float(row.get("refining_rate")),
                    _to_text(row.get("date_updated")),
                    _to_text(row.get("last_synced")),
                    row_hash,
                    raw_json,
                    loaded_at,
                )
            )
        upserted = _upsert_clean_rows(
            conn_clean,
            "clean_trademc_companies",
            company_upserts,
            """
            INSERT INTO clean_trademc_companies (
                source_pk, status, company_name, refining_rate, date_updated, last_synced,
                row_hash, raw_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_pk) DO UPDATE SET
                status=excluded.status,
                company_name=excluded.company_name,
                refining_rate=excluded.refining_rate,
                date_updated=excluded.date_updated,
                last_synced=excluded.last_synced,
                row_hash=excluded.row_hash,
                raw_json=excluded.raw_json,
                loaded_at=excluded.loaded_at
            """,
        )
        company_dataset_hash = hashlib.sha256("|".join(sorted(company_hash_feed)).encode("utf-8")).hexdigest() if company_hash_feed else hashlib.sha256(b"").hexdigest()
        _save_metric("trademc_companies", len(company_rows), upserted, company_dataset_hash)

        # TradeMC weight transactions.
        weight_rows = _load_rows(conn_ledger, "trademc_weight_transactions")
        weight_upserts: List[Tuple[Any, ...]] = []
        weight_hash_feed: List[str] = []
        loaded_at = _utc_now_iso()
        for row in weight_rows:
            source_pk = row.get("id")
            if source_pk is None:
                continue
            row_hash, raw_json = _row_hash_and_json(row)
            weight_hash_feed.append(row_hash)
            weight_upserts.append(
                (
                    int(source_pk),
                    int(row["company_id"]) if row.get("company_id") is not None else None,
                    _to_text(row.get("type")),
                    _to_float(row.get("weight")),
                    _to_float(row.get("rolling_balance")),
                    _to_text(row.get("transaction_timestamp")),
                    _to_text(row.get("date_updated")),
                    _to_text(row.get("last_synced")),
                    row_hash,
                    raw_json,
                    loaded_at,
                )
            )
        upserted = _upsert_clean_rows(
            conn_clean,
            "clean_trademc_weight_transactions",
            weight_upserts,
            """
            INSERT INTO clean_trademc_weight_transactions (
                source_pk, company_id, type, weight, rolling_balance, transaction_timestamp,
                date_updated, last_synced, row_hash, raw_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_pk) DO UPDATE SET
                company_id=excluded.company_id,
                type=excluded.type,
                weight=excluded.weight,
                rolling_balance=excluded.rolling_balance,
                transaction_timestamp=excluded.transaction_timestamp,
                date_updated=excluded.date_updated,
                last_synced=excluded.last_synced,
                row_hash=excluded.row_hash,
                raw_json=excluded.raw_json,
                loaded_at=excluded.loaded_at
            """,
        )
        weight_dataset_hash = hashlib.sha256("|".join(sorted(weight_hash_feed)).encode("utf-8")).hexdigest() if weight_hash_feed else hashlib.sha256(b"").hexdigest()
        _save_metric("trademc_weight_transactions", len(weight_rows), upserted, weight_dataset_hash)

        # Persist metrics + finalize.
        cur_clean.execute("DELETE FROM pipeline_batch_metrics WHERE batch_id = ?", (batch_id,))
        for dataset, m in metrics.items():
            cur_clean.execute(
                """
                INSERT INTO pipeline_batch_metrics (
                    batch_id, dataset, records_upserted, records_seen, dataset_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    dataset,
                    int(m.get("records_upserted", 0)),
                    int(m.get("records_seen", 0)),
                    str(m.get("dataset_hash", "")),
                    _utc_now_iso(),
                ),
            )

        cur_clean.execute(
            """
            UPDATE pipeline_batches
            SET status='ok', completed_at=?
            WHERE id=?
            """,
            (_utc_now_iso(), batch_id),
        )
        conn_clean.commit()
        conn_ledger.close()
        conn_pmx.close()
        conn_clean.close()
        return {
            "ok": True,
            "batch_id": batch_id,
            "clean_db_path": target_db,
            "metrics": metrics,
        }
    except Exception as exc:
        cur_clean.execute(
            """
            UPDATE pipeline_batches
            SET status='error', completed_at=?, error=?
            WHERE id=?
            """,
            (_utc_now_iso(), str(exc), batch_id),
        )
        conn_clean.commit()
        conn_clean.close()
        return {"ok": False, "batch_id": batch_id, "error": str(exc), "clean_db_path": target_db}

