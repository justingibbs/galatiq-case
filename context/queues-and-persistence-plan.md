# Invoice Pipeline — Queues, Vendor Table, Stock Deduction & Persistence

## Context

Currently the pipeline is fully automated and ephemeral: invoices are extracted, validated, approved, and "paid" — but nothing is saved to the database, stock is never deducted, and there's no way to handle ambiguous matches or stock shortages. This plan adds real business logic with three distinct outcomes, persistent storage, and human-in-the-loop review queues.

---

## New Pipeline Flow

```
IngestNode → MatchNode (NEW) → ValidateNode → ApproveNode → PayNode → End[COMPLETED]
                 ↓                   ↓              ↓
          End[NEEDS_MATCH_REVIEW]  End[NEEDS_STOCK_REVIEW]  End[REJECTED]
```

- **MatchNode**: LLM matches extracted items/vendor against known inventory + vendors table with confidence scores. High confidence (>=0.8) → proceed. Low → pause in review queue.
- **ValidateNode** (modified): Insufficient stock now routes to stock review queue instead of auto-rejecting. Critical flags (out_of_stock=0, negative qty, data integrity) still reject.
- **PayNode** (modified): Deducts inventory stock, records transaction in `processed_invoices`, links vendor.

**Queue pause mechanism**: pydantic-graph runs synchronously, so queued invoices `End()` with a special stage. User actions via API trigger a **new** graph run starting from the appropriate resume node.

---

## Phase 1: Database (`app/database.py`)

**Split `init_db()` into two functions:**
- `ensure_db(db_path)` — uses `CREATE TABLE IF NOT EXISTS`, safe for every startup. Inserts seed data only if tables are empty.
- `reset_database(db_path)` — drops all tables, recreates with seed data. Called from reset API.

**New tables:**

```sql
CREATE TABLE vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    email TEXT,
    phone TEXT,
    address TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE processed_invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_number TEXT,
    vendor_name TEXT NOT NULL,
    filename TEXT NOT NULL,
    total_amount REAL NOT NULL,
    currency TEXT DEFAULT 'USD',
    transaction_id TEXT NOT NULL UNIQUE,
    line_items_json TEXT NOT NULL,
    tax_amount REAL,
    subtotal REAL,
    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
    invoice_date TEXT,
    due_date TEXT
);

CREATE TABLE review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_id TEXT NOT NULL UNIQUE,
    review_type TEXT NOT NULL,          -- 'match_review' or 'stock_review'
    extracted_json TEXT NOT NULL,
    match_result_json TEXT,
    validation_json TEXT,
    stock_issues_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT
);
```

**Vendor seed data:** ~9 vendors from existing test invoices (Widgets Inc., Gadgets Co., Precision Parts Ltd., etc.)

**New functions:** `get_all_inventory_items()`, `get_all_vendors()`, `create_vendor()`, `deduct_stock()`, `record_processed_invoice()`, `get_processed_invoices()`, `save_review_queue_item()`, `get_review_queue()`, `resolve_review_queue_item()`, `reset_database()`

**Update `server.py`:** Change `init_db(DB_PATH)` → `ensure_db(DB_PATH)` in lifespan.

---

## Phase 2: Models (`app/models.py`)

**New models:**
- `ItemMatch` — extracted_description, matched_item, confidence (0-1), alternatives list
- `VendorMatch` — extracted_name, matched_vendor_id, matched_vendor_name, confidence, alternatives
- `MatchResult` — vendor_match, item_matches list, all_high_confidence bool
- `StockCheckResult` — item, requested_quantity, available_stock, shortfall

**Modified enums/models:**
- `PipelineStage`: add `MATCHING`, `NEEDS_MATCH_REVIEW`, `NEEDS_STOCK_REVIEW`, `BACKORDERED`
- `InvoiceStatus`: add `match_result: MatchResult | None`, `stock_issues: list[StockCheckResult] | None`

---

## Phase 3: Matching Agent (`app/agents.py`)

**New `get_matching_agent()`** with `NativeOutput(MatchResult)`:
- Receives extracted invoice data + list of known inventory items + list of known vendors
- Returns confidence scores for each item and vendor match
- Handles fuzzy matching, OCR errors, formatting variations
- `MATCH_CONFIDENCE_THRESHOLD = 0.8` constant

---

## Phase 4: Graph Nodes (`app/graph.py`)

**Add to `PipelineState`:** `invoice_id`, `match_result`, `stock_issues`
**Add to `PipelineResult`:** `match_result`, `stock_issues`

**New `MatchNode`:**
1. Query DB for all inventory items and vendors
2. Build prompt with extracted data + known items/vendors
3. Call matching agent
4. If `all_high_confidence`: remap extracted item descriptions to matched names → `ValidateNode`
5. Else: save to `review_queue` table → `End(NEEDS_MATCH_REVIEW)`

**Modify `IngestNode`:** return `MatchNode` instead of `ValidateNode`

**Modify `ValidateNode`:**
- Separate insufficient_stock flags from critical flags
- Insufficient stock → compute `StockCheckResult` list, save to `review_queue` → `End(NEEDS_STOCK_REVIEW)`
- Critical flags (out_of_stock=0, negative_qty, data integrity) → `RejectNode`
- Clean → `ApproveNode`

**Modify `PayNode`:**
1. Deduct stock for each line item via `deduct_stock()`
2. Record in `processed_invoices` via `record_processed_invoice()`
3. Generate payment (same as current)

**New resume functions:**
- `resume_after_match_review()` — starts graph at `ValidateNode` with corrected data
- `resume_after_stock_review()` — starts graph at `ApproveNode` (for approve/partial) or directly rejects

**Update graph definition:** add `MatchNode` to nodes list.

---

## Phase 5: API Endpoints (`app/api.py`)

**New endpoints:**

| Endpoint | Purpose |
|----------|---------|
| `POST /api/invoices/{id}/resolve-match` | User selects correct items/vendor from alternatives or creates new vendor. Body: `{item_mappings, vendor_name, create_vendor?}`. Resumes pipeline from ValidateNode. |
| `POST /api/invoices/{id}/resolve-stock` | User picks action. Body: `{action: "reject"\|"partial"\|"backorder", adjusted_quantities?}`. Reject → mark rejected. Partial → adjust quantities, recalculate totals, resume from ApproveNode. Backorder → mark as BACKORDERED (holds until stock available). |
| `GET /api/review-queue` | List pending review items. Optional `?type=match_review\|stock_review` filter. |
| `GET /api/vendors` | List all vendors. |
| `POST /api/vendors` | Create vendor. Body: `{name, email?, phone?, address?}`. |
| `GET /api/processed-invoices` | List all completed transactions. |
| `POST /api/reset-database` | Reset all tables to seed state, reset InvoiceStore to PENDING. |

**Modify existing:** Allow reprocessing from `NEEDS_MATCH_REVIEW` and `NEEDS_STOCK_REVIEW` stages.

---

## Phase 6: Pages & Templates

**New page routes (`app/pages.py`):**
- `GET /review-queue` — queue listing with filter tabs
- `GET /processed` — processed invoices table
- `GET /vendors` — vendor management table + add form

**Modified templates:**
- `base.html` — add nav links (Review Queue, Processed, Vendors) + Reset DB button with `hx-confirm`
- `pipeline_steps.html` — add "Match" step, handle new terminal states with warning styling
- `invoice_card.html` — add badges for `matching`, `needs_match_review`, `needs_stock_review`, `backordered` + "Review" button linking to detail page
- `detail_content.html` / `results_panel.html` — add match results panel showing confidence scores; add match review form (item dropdowns + vendor field) and stock review form (reject/partial/backorder buttons + quantity inputs) using HTMX

**New templates:**
- `review_queue.html` — filterable list of pending review items
- `processed.html` — table of processed invoices from DB
- `vendors.html` — vendor table + "Add Vendor" HTMX form
- `partials/match_results.html` — confidence display with color coding
- `partials/match_review_form.html` — dropdowns for alternatives + vendor field
- `partials/stock_review_form.html` — action buttons + quantity adjustment inputs

---

## Files Modified (summary)

| File | Changes |
|------|---------|
| `app/database.py` | Split init_db → ensure_db + reset_database. Add vendors/processed_invoices/review_queue tables. Add ~12 new functions. |
| `app/models.py` | Add ItemMatch, VendorMatch, MatchResult, StockCheckResult. Extend PipelineStage, InvoiceStatus. |
| `app/agents.py` | Add get_matching_agent() + MATCH_CONFIDENCE_THRESHOLD constant. |
| `app/graph.py` | Add MatchNode. Modify IngestNode, ValidateNode, PayNode. Add PipelineState/Result fields. Add resume runners. |
| `app/api.py` | Add 7 new endpoints. Modify _process_invoice for new fields. |
| `app/pages.py` | Add 3 new page routes. |
| `app/store.py` | Add reset_all() method. |
| `server.py` | Change init_db → ensure_db. |
| `templates/base.html` | Nav links + reset button. |
| `templates/partials/*` | Modify pipeline_steps, invoice_card, detail_content, results_panel. |
| 6 new template files | review_queue.html, processed.html, vendors.html, 3 new partials. |

---

## Verification

1. **Start server** — `python server.py` should create all tables without dropping existing data
2. **Process a clean invoice** (e.g. invoice_1001.txt with WidgetA+WidgetB) — should flow through Match → Validate → Approve → Pay → COMPLETED. Check `processed_invoices` table has a row. Check inventory stock decremented.
3. **Process an invoice with unknown items** — should pause at NEEDS_MATCH_REVIEW. Visit detail page, see match review form. Resolve by picking alternatives. Pipeline resumes.
4. **Process an invoice exceeding stock** — should pause at NEEDS_STOCK_REVIEW. Test all three actions: reject, partial fill, backorder.
5. **Visit /review-queue** — see pending items with correct types.
6. **Visit /processed** — see completed transactions.
7. **Visit /vendors** — see seeded vendors, add a new one.
8. **Click Reset DB** — all tables reset, inventory back to seed values, all invoices back to PENDING.
