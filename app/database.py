from __future__ import annotations

import sqlite3
from pathlib import Path

INVENTORY_SEED = [
    ("WidgetA", 15),
    ("WidgetB", 10),
    ("GadgetX", 5),
    ("FakeItem", 0),
]

VENDOR_SEED = [
    ("Widgets Inc.", None, None, None),
    ("Gadgets Co.", None, None, None),
    ("Fraudster LLC", None, None, None),
    ("Precision Parts Ltd.", None, None, None),
    ("Global Supply Chain Partners", None, None, None),
    ("Acme Industrial Supplies", None, None, None),
    ("MegaWidgets Corp", None, None, None),
    ("NoProd Industries", None, None, None),
    ("Consolidated Materials Group", None, None, None),
    ("Summit Manufacturing Co.", None, None, None),
    ("QuickShip Distributers", None, None, None),
    ("Atlas Industrial Supply", None, None, None),
    ("TechParts International", None, None, None),
    ("Reliable Components Inc.", None, None, None),
]


def _create_tables(cur: sqlite3.Cursor) -> None:
    """Create all tables using IF NOT EXISTS."""
    cur.execute(
        "CREATE TABLE IF NOT EXISTS inventory ("
        "  item TEXT PRIMARY KEY,"
        "  stock INTEGER NOT NULL"
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS vendors ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL UNIQUE,"
        "  email TEXT,"
        "  phone TEXT,"
        "  address TEXT,"
        "  created_at TEXT NOT NULL DEFAULT (datetime('now'))"
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS processed_invoices ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  invoice_number TEXT,"
        "  vendor_name TEXT NOT NULL,"
        "  filename TEXT NOT NULL,"
        "  total_amount REAL NOT NULL,"
        "  currency TEXT DEFAULT 'USD',"
        "  transaction_id TEXT NOT NULL UNIQUE,"
        "  line_items_json TEXT NOT NULL,"
        "  tax_amount REAL,"
        "  subtotal REAL,"
        "  processed_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  invoice_date TEXT,"
        "  due_date TEXT"
        ")"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS review_queue ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  invoice_id TEXT NOT NULL UNIQUE,"
        "  review_type TEXT NOT NULL,"
        "  extracted_json TEXT NOT NULL,"
        "  match_result_json TEXT,"
        "  validation_json TEXT,"
        "  stock_issues_json TEXT,"
        "  status TEXT NOT NULL DEFAULT 'pending',"
        "  created_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  resolved_at TEXT"
        ")"
    )


def _seed_if_empty(cur: sqlite3.Cursor) -> None:
    """Insert seed data only if tables are empty."""
    row = cur.execute("SELECT COUNT(*) FROM inventory").fetchone()
    if row[0] == 0:
        cur.executemany(
            "INSERT INTO inventory (item, stock) VALUES (?, ?)", INVENTORY_SEED
        )

    row = cur.execute("SELECT COUNT(*) FROM vendors").fetchone()
    if row[0] == 0:
        cur.executemany(
            "INSERT INTO vendors (name, email, phone, address) VALUES (?, ?, ?, ?)",
            VENDOR_SEED,
        )


def ensure_db(db_path: str | Path = "inventory.db") -> None:
    """Create tables if they don't exist and seed empty tables. Safe for every startup."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    _create_tables(cur)
    _seed_if_empty(cur)
    conn.commit()
    conn.close()


def reset_database(db_path: str | Path = "inventory.db") -> None:
    """Drop all tables, recreate with seed data."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    for table in ("review_queue", "processed_invoices", "vendors", "inventory"):
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    _create_tables(cur)
    _seed_if_empty(cur)
    conn.commit()
    conn.close()


# Keep backward compat alias
init_db = reset_database


# --------------- inventory helpers ---------------

def check_item_stock(item_name: str, db_path: str | Path = "inventory.db") -> tuple[bool, int]:
    """Check if an item exists and return (exists, stock_count)."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT stock FROM inventory WHERE item = ?", (item_name,))
    row = cur.fetchone()
    conn.close()
    if row is None:
        return False, 0
    return True, row[0]


def get_all_inventory_items(db_path: str | Path = "inventory.db") -> list[dict]:
    """Return all inventory items as list of {item, stock} dicts."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT item, stock FROM inventory ORDER BY item").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def deduct_stock(item_name: str, quantity: float, db_path: str | Path = "inventory.db") -> bool:
    """Deduct quantity from item stock. Returns True if successful."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT stock FROM inventory WHERE item = ?", (item_name,))
    row = cur.fetchone()
    if row is None:
        conn.close()
        return False
    new_stock = row[0] - int(quantity)
    if new_stock < 0:
        conn.close()
        return False
    cur.execute("UPDATE inventory SET stock = ? WHERE item = ?", (new_stock, item_name))
    conn.commit()
    conn.close()
    return True


# --------------- vendor helpers ---------------

def get_all_vendors(db_path: str | Path = "inventory.db") -> list[dict]:
    """Return all vendors as list of dicts."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, email, phone, address, created_at FROM vendors ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_vendor(
    name: str,
    db_path: str | Path = "inventory.db",
    email: str | None = None,
    phone: str | None = None,
    address: str | None = None,
) -> int:
    """Create a new vendor. Returns the new vendor id."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO vendors (name, email, phone, address) VALUES (?, ?, ?, ?)",
        (name, email, phone, address),
    )
    conn.commit()
    vendor_id = cur.lastrowid
    conn.close()
    return vendor_id


# --------------- processed invoices ---------------

def record_processed_invoice(
    invoice_number: str | None,
    vendor_name: str,
    filename: str,
    total_amount: float,
    currency: str,
    transaction_id: str,
    line_items_json: str,
    tax_amount: float | None = None,
    subtotal: float | None = None,
    invoice_date: str | None = None,
    due_date: str | None = None,
    db_path: str | Path = "inventory.db",
) -> int:
    """Record a processed invoice. Returns the row id."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO processed_invoices "
        "(invoice_number, vendor_name, filename, total_amount, currency, "
        " transaction_id, line_items_json, tax_amount, subtotal, invoice_date, due_date) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            invoice_number, vendor_name, filename, total_amount, currency,
            transaction_id, line_items_json, tax_amount, subtotal,
            invoice_date, due_date,
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def get_processed_invoices(db_path: str | Path = "inventory.db") -> list[dict]:
    """Return all processed invoices."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM processed_invoices ORDER BY processed_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --------------- review queue ---------------

def save_review_queue_item(
    invoice_id: str,
    review_type: str,
    extracted_json: str,
    match_result_json: str | None = None,
    validation_json: str | None = None,
    stock_issues_json: str | None = None,
    db_path: str | Path = "inventory.db",
) -> int:
    """Save an item to the review queue. Returns the row id."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    # Upsert: replace if same invoice_id already queued
    cur.execute(
        "INSERT OR REPLACE INTO review_queue "
        "(invoice_id, review_type, extracted_json, match_result_json, "
        " validation_json, stock_issues_json, status) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
        (
            invoice_id, review_type, extracted_json,
            match_result_json, validation_json, stock_issues_json,
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def get_review_queue(
    review_type: str | None = None,
    status: str = "pending",
    db_path: str | Path = "inventory.db",
) -> list[dict]:
    """Return review queue items, optionally filtered by type."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    if review_type:
        rows = conn.execute(
            "SELECT * FROM review_queue WHERE status = ? AND review_type = ? "
            "ORDER BY created_at DESC",
            (status, review_type),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM review_queue WHERE status = ? ORDER BY created_at DESC",
            (status,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_review_queue_item(invoice_id: str, db_path: str | Path = "inventory.db") -> dict | None:
    """Return a single review queue item by invoice_id."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM review_queue WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def resolve_review_queue_item(
    invoice_id: str, db_path: str | Path = "inventory.db"
) -> None:
    """Mark a review queue item as resolved."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "UPDATE review_queue SET status = 'resolved', resolved_at = datetime('now') "
        "WHERE invoice_id = ?",
        (invoice_id,),
    )
    conn.commit()
    conn.close()
