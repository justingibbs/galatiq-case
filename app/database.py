from __future__ import annotations

import sqlite3
from pathlib import Path

SEED_DATA = [
    ("WidgetA", 15),
    ("WidgetB", 10),
    ("GadgetX", 5),
    ("FakeItem", 0),
]


def init_db(db_path: str | Path = "inventory.db") -> None:
    """Create/reset the inventory database with seed data."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS inventory")
    cur.execute(
        "CREATE TABLE inventory (item TEXT PRIMARY KEY, stock INTEGER NOT NULL)"
    )
    cur.executemany("INSERT INTO inventory (item, stock) VALUES (?, ?)", SEED_DATA)
    conn.commit()
    conn.close()


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
