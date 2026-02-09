# System Design: Invoice Processing Pipeline

A deep-dive into the architecture, agent logic, data flow, and design decisions behind Acme Corp's automated invoice processing system.

---

## Table of Contents

- [High-Level Architecture](#high-level-architecture)
- [Pipeline State Machine](#pipeline-state-machine)
- [Agent Deep-Dives](#agent-deep-dives)
  - [Extraction Agent](#1-extraction-agent)
  - [Matching Agent](#2-matching-agent)
  - [Approval Agent](#3-approval-agent)
- [Pipeline Nodes](#pipeline-nodes)
- [Human-in-the-Loop](#human-in-the-loop)
- [Data Models](#data-models)
- [Database Schema](#database-schema)
- [API Surface](#api-surface)
- [Real-Time Updates](#real-time-updates)
- [Invoice Test Corpus](#invoice-test-corpus)

---

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           BROWSER (HTMX + DaisyUI)                      │
│                                                                          │
│   Wizard UI ◄──── SSE (Server-Sent Events) ────── Real-time updates     │
│       │                                                                  │
│       ▼                                                                  │
│   REST API calls (POST /api/batch/import, /resolve-match, etc.)         │
└──────────────────────┬───────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         FastAPI  (server.py)                             │
│                                                                          │
│  ┌─────────┐   ┌──────────┐   ┌─────────┐   ┌───────────────────────┐  │
│  │ Pages   │   │ REST API │   │   SSE   │   │ Jinja2 Templates      │  │
│  │ (HTML)  │   │ (JSON +  │   │ Streams │   │ (HTMX partials)       │  │
│  │         │   │  HTMX)   │   │         │   │                       │  │
│  └────┬────┘   └────┬─────┘   └────┬────┘   └───────────────────────┘  │
│       │              │              │                                    │
│       └──────────────┼──────────────┘                                   │
│                      ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    InvoiceStore (in-memory)                       │   │
│  │                                                                   │   │
│  │  invoices: dict[id → InvoiceStatus]                              │   │
│  │  subscribers: per-invoice + batch SSE queues                     │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                              │                                           │
│                              ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                     pydantic-graph Pipeline                       │   │
│  │                                                                   │   │
│  │  IngestNode → MatchNode → ValidateNode → ApproveNode → PayNode  │   │
│  │                                                                   │   │
│  │     ┌────────────┐  ┌────────────┐  ┌────────────────┐          │   │
│  │     │ Extraction │  │  Matching  │  │   Approval     │          │   │
│  │     │   Agent    │  │   Agent    │  │    Agent       │          │   │
│  │     │  (Gemini)  │  │  (Gemini)  │  │   (Gemini)    │          │   │
│  │     └────────────┘  └────────────┘  └────────────────┘          │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
│                              │                                           │
│                              ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                       SQLite Database                             │   │
│  │                                                                   │   │
│  │  inventory │ vendors │ processed_invoices │ review_queue          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

**Technology choices:**

| Layer | Technology | Why |
|-------|-----------|-----|
| LLM | Gemini 2.0 Flash via PydanticAI | Native structured output, fast inference |
| Orchestration | pydantic-graph | Type-safe state machine, clean pause/resume |
| Web | FastAPI + Jinja2 + HTMX | Server-rendered partials, no JS framework needed |
| Styling | DaisyUI (Tailwind) | Rapid prototyping with semantic component classes |
| Real-time | SSE via sse-starlette | Simpler than WebSockets for one-way push |
| Database | SQLite | Zero-config, file-based, sufficient for prototype |
| PDF | pdfplumber | Reliable text extraction from PDF invoices |

---

## Pipeline State Machine

The pipeline is built as a **directed graph** using `pydantic-graph`. Each node is a `@dataclass` with an async `run()` method. Return types define valid edges — the framework enforces them at runtime.

### State Diagram

```
                         ┌──────────┐
                         │ PENDING  │
                         └────┬─────┘
                              │
                              ▼
                       ┌─────────────┐
                       │  INGESTING  │
                       └──────┬──────┘
                              │
                         ┌────┴────┐
                         │ FAILED  │ ◄── (parse error)
                         └─────────┘
                              │ (success)
                              ▼
                       ┌─────────────┐
                       │  MATCHING   │
                       └──────┬──────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               │               ▼
   ┌───────────────────┐     │       ┌─────────────┐
   │ NEEDS_MATCH_REVIEW│     │       │   MATCHED   │
   │  (human review)   │     │       │ (stop here  │
   └────────┬──────────┘     │       │  in wizard) │
            │                │       └──────┬──────┘
            │  (resolved)    │              │
            └────────────────┘              │
                              │             │
                              ▼             ▼
                       ┌─────────────┐
                       │ VALIDATING  │
                       └──────┬──────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               │               ▼
    ┌──────────────────┐      │       ┌─────────────┐
    │NEEDS_STOCK_REVIEW│      │       │  REJECTED   │ ◄── (critical flags)
    │  (human review)  │      │       └─────────────┘
    └────────┬─────────┘      │
             │                │
             │  (resolved)    │
             └────────────────┘
                              │ (valid)
                              ▼
                       ┌─────────────┐
                       │  APPROVING  │
                       └──────┬──────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            ┌─────────────┐     ┌─────────────┐
            │   PAYING    │     │  REJECTED   │
            └──────┬──────┘     └─────────────┘
                   │
                   ▼
            ┌─────────────┐
            │  COMPLETED  │
            └─────────────┘
```

### Pipeline Stages (enum)

| Stage | Terminal? | Description |
|-------|-----------|-------------|
| `PENDING` | No | Initial state, not yet processed |
| `INGESTING` | No | Extracting structured data from raw file |
| `MATCHING` | No | Fuzzy-matching vendor + items against DB |
| `MATCHED` | No | All matches confident, awaiting fulfillment |
| `VALIDATING` | No | Checking stock levels and data integrity |
| `APPROVING` | No | AI risk assessment in progress |
| `PAYING` | No | Deducting stock, recording transaction |
| `COMPLETED` | Yes | Successfully paid |
| `REJECTED` | Yes | Denied by validation or approval |
| `FAILED` | Yes | Unrecoverable error |
| `NEEDS_MATCH_REVIEW` | Paused | Low-confidence match, awaiting human |
| `NEEDS_STOCK_REVIEW` | Paused | Insufficient stock, awaiting human |
| `BACKORDERED` | Yes | Human chose to backorder stock issue |

### Two-Phase Execution (Wizard Mode)

The web UI splits processing into two phases so humans can review between them:

```
 PHASE 1: IMPORT                      PHASE 2: FULFILL
┌────────────────────┐               ┌────────────────────────┐
│ Ingest → Match     │    human      │ Validate → Approve     │
│                    │───review───▶  │          → Pay         │
│ Stops at MATCHED   │   (step 2)   │                        │
│ or NEEDS_MATCH     │               │ From MATCHED invoices  │
└────────────────────┘               └────────────────────────┘
     Wizard Step 1                     Wizard Step 3
                        Step 2:
                   Resolve matches
```

---

## Agent Deep-Dives

All three agents use **Google Gemini 2.0 Flash** via PydanticAI with `NativeOutput` for structured responses. Each is instantiated lazily with `@lru_cache` to defer API key validation.

### 1. Extraction Agent

**Purpose:** Parse raw invoice text (any format) into a structured `ExtractedInvoice`.

**Input:** Raw text string (from TXT, JSON, CSV, XML, or PDF-extracted text)

**Output:** `ExtractedInvoice` (structured Pydantic model)

```
                   ┌─────────────────────────┐
 Raw invoice text  │    Extraction Agent      │  ExtractedInvoice
─────────────────▶ │                          │─────────────────▶
 (any format)      │  • OCR error correction  │   • invoice_number
                   │  • Name normalization    │   • vendor_name
                   │  • Date standardization  │   • line_items[]
                   │  • Format detection      │   • total_amount
                   │  • Currency inference    │   • tax, dates, etc.
                   └─────────────────────────┘
```

**Extraction rules:**

| Rule | Detail |
|------|--------|
| OCR correction | Fixes common errors: `O`→`0`, `l`→`1`, `Payble`→`Payable` |
| Name normalization | Strips whitespace variations: `Widget A` → `WidgetA` |
| Date normalization | Converts all date formats to `YYYY-MM-DD` |
| Currency default | Assumes `USD` if no currency specified |
| Line item capture | Extracts ALL items including shipping, fees, discounts |
| Total preservation | Uses the stated total, does not recalculate |

**Retries:** 2 (with exponential backoff via PydanticAI)

---

### 2. Matching Agent

**Purpose:** Fuzzy-match extracted vendor names and line items against known database records.

**Input:** Extracted invoice data + full inventory list + full vendor list (from DB)

**Output:** `MatchResult` with confidence scores per item and vendor

```
                     ┌──────────────────────────────────┐
 ExtractedInvoice    │         Matching Agent            │   MatchResult
 + inventory list  ▶ │                                    │ ▶  • vendor_match
 + vendor list       │  For each line item:               │    • item_matches[]
                     │   → score against all inventory   │    • all_high_confidence
                     │   → provide up to 3 alternatives  │
                     │                                    │
                     │  For vendor:                       │
                     │   → score against all vendors     │
                     │   → provide up to 3 alternatives  │
                     └──────────────────────────────────┘
```

**Confidence scoring rubric (0.0–1.0):**

| Score Range | Meaning | Example |
|-------------|---------|---------|
| **1.0** | Exact match after normalization | `"WidgetA"` → `WidgetA` |
| **0.8–0.99** | Very likely, minor variation | `"Widget A"` → `WidgetA` (spacing) |
| **0.5–0.79** | Possible but uncertain | `"Widget Co"` → `Widgets Inc.` (partial) |
| **0.0–0.49** | No good match found | `"SuperGizmo"` → ∅ |

**Special handling:**

| Case | Behavior |
|------|----------|
| Service items (Shipping, Rush Fee, Discount) | Confidence = 1.0, matched to self |
| Non-inventory charges | Passed through without matching |
| `all_high_confidence` flag | `true` only if EVERY item AND vendor ≥ 0.8 |

**Decision logic after matching:**

```
              all_high_confidence?
              ┌──────┴──────┐
             YES             NO
              │               │
              ▼               ▼
  Remap item names     Save to review_queue
  to matched names     (type: match_review)
              │               │
              ▼               ▼
  Continue to          END → NEEDS_MATCH_REVIEW
  ValidateNode         (pause for human)
```

**Match threshold:** 0.8 (items below this trigger human review)

**Retries:** 2

---

### 3. Approval Agent

**Purpose:** Act as a VP-level financial controller, evaluating invoices for risk and fraud.

**Input:** Extracted invoice + validation results + matched line items

**Output:** `ApprovalDecision` with reasoning, risk level, and manual review flag

```
                      ┌──────────────────────────────────┐
 ExtractedInvoice     │         Approval Agent            │   ApprovalDecision
 + ValidationResult ▶ │                                    │ ▶  • approved (bool)
 + line items         │  Evaluates:                       │    • reasoning (str)
                      │   • Amount thresholds             │    • risk_level
                      │   • Validation flags              │    • requires_manual_review
                      │   • Fraud indicators              │
                      └──────────────────────────────────┘
```

**Full decision rubric:**

```
START
  │
  ▼
total_amount > $10,000? ──YES──▶ risk = HIGH, requires_manual_review = true
  │ NO
  ▼
has "out_of_stock" flag? ──YES──▶ REJECT (cannot fulfill)
  │ NO
  ▼
has "negative_quantity"? ──YES──▶ REJECT (fraud indicator)
  │ NO
  ▼
has "unknown_item"? ──YES──▶ risk = MEDIUM, requires_manual_review = true
  │ NO
  ▼
has "insufficient_stock"? ──YES──▶ approve if reasonable, else manual review
  │ NO
  ▼
fraud indicators detected?
  │
  ├─ Vendor name suggests fraud ──▶ AUTO-REJECT
  │  (e.g., "Fraudster LLC")
  │
  ├─ Urgent payment demands ──▶ AUTO-REJECT
  │  or pressure language
  │
  ├─ Unusually high amount ──▶ AUTO-REJECT
  │  for simple items
  │
  ├─ Due date in the past ──▶ AUTO-REJECT
  │  or "immediate" terms
  │
  └─ Items with zero stock ──▶ AUTO-REJECT
     that shouldn't be ordered
  │
  │ (no flags, no fraud)
  ▼
APPROVE — risk = LOW
```

**Risk levels:**

| Level | Criteria |
|-------|----------|
| `low` | No flags, total < $10,000 |
| `medium` | Unknown items or minor stock issues |
| `high` | Total > $10,000 or multiple flags |

**Retries:** 2

---

## Pipeline Nodes

Each node is a `@dataclass` implementing `BaseNode` from `pydantic-graph`.

### Node Responsibilities

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  IngestNode                                                          │
│  ├─ Read file (format-aware: TXT/JSON/CSV/XML/PDF)                  │
│  ├─ Call Extraction Agent → ExtractedInvoice                        │
│  └─ Route: success → MatchNode │ error → End(FAILED)               │
│                                                                      │
│  MatchNode                                                           │
│  ├─ Load inventory + vendors from SQLite                            │
│  ├─ Call Matching Agent → MatchResult                               │
│  ├─ If all confident: remap item names, route → ValidateNode        │
│  ├─ If stop_after_match: End(MATCHED)                               │
│  └─ If low confidence: save review_queue → End(NEEDS_MATCH_REVIEW) │
│                                                                      │
│  ValidateNode                                                        │
│  ├─ Check: negative quantities (critical)                           │
│  ├─ Check: unknown items, out-of-stock (critical), low stock        │
│  ├─ Check: data integrity (negative total, missing vendor)          │
│  ├─ If critical flags → RejectNode                                  │
│  ├─ If stock issues → save review_queue → End(NEEDS_STOCK_REVIEW)  │
│  └─ If valid → ApproveNode                                          │
│                                                                      │
│  ApproveNode                                                         │
│  ├─ Call Approval Agent → ApprovalDecision                          │
│  ├─ If approved → PayNode                                           │
│  └─ If rejected → RejectNode                                        │
│                                                                      │
│  PayNode                                                             │
│  ├─ Deduct stock from inventory (per matched item)                  │
│  ├─ Generate mock transaction (TXN-{hex})                           │
│  ├─ Record in processed_invoices table                              │
│  └─ End(COMPLETED)                                                   │
│                                                                      │
│  RejectNode                                                          │
│  └─ End(REJECTED) with reason string                                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Validation Checks (ValidateNode detail)

| # | Check | Flag Type | Severity | Result |
|---|-------|-----------|----------|--------|
| 1 | `quantity < 0` on any line item | `negative_quantity` | Critical | → Reject |
| 2 | Item not found in inventory DB | `unknown_item` | Non-critical | Passed to approval |
| 3 | Item exists but `stock = 0` | `out_of_stock` | Critical | → Reject |
| 4 | `quantity > available stock` | `insufficient_stock` | Non-critical | → Stock review queue |
| 5 | `total_amount < 0` | `data_integrity` | Critical | → Reject |
| 6 | `vendor_name` empty or null | `data_integrity` | Non-critical | Passed to approval |

---

## Human-in-the-Loop

The pipeline pauses at two points where AI confidence is insufficient. Humans resolve issues through the web UI, and the pipeline resumes from where it left off.

### Match Review

```
Pipeline pauses at NEEDS_MATCH_REVIEW
          │
          ▼
┌────────────────────────────────────┐
│        Human Review UI             │
│                                    │
│  For each low-confidence item:     │
│   • See extracted name             │
│   • See best match + confidence    │
│   • See up to 3 alternatives       │
│   • Select correct match OR        │
│     type a custom name             │
│                                    │
│  For vendor:                       │
│   • Confirm or correct vendor      │
│   • Option to create new vendor    │
│                                    │
│  Actions: [Resolve] or [Reject]    │
└──────────────┬─────────────────────┘
               │
     ┌─────────┴─────────┐
     ▼                   ▼
  Resolve             Reject
     │                   │
     ▼                   ▼
  POST /api/          Stage →
  resolve-match       REJECTED
     │
     ▼
  Apply corrections
  to ExtractedInvoice
     │
     ▼
  Stage → MATCHED
  (resumes in fulfill phase)
```

### Stock Review

```
Pipeline pauses at NEEDS_STOCK_REVIEW
          │
          ▼
┌──────────────────────────────────────┐
│         Human Review UI              │
│                                      │
│  For each insufficient-stock item:   │
│   • Item name                        │
│   • Requested quantity               │
│   • Available stock                  │
│   • Shortfall amount                 │
│                                      │
│  Actions:                            │
│   [Reject] – deny the invoice        │
│   [Backorder] – accept, fill later  │
│   [Partial] – adjust quantities     │
│              to available stock      │
└──────────────┬───────────────────────┘
               │
     ┌─────────┼─────────┐
     ▼         ▼         ▼
  Reject   Backorder   Partial
     │         │         │
     ▼         ▼         ▼
  REJECTED  BACKORDERED  Adjust qtys,
                         recalculate
                         totals,
                         resume from
                         ApproveNode
```

---

## Data Models

### Core Models Diagram

```
ExtractedInvoice
├── invoice_number: str?
├── vendor_name: str
├── invoice_date: str?          (YYYY-MM-DD)
├── due_date: str?
├── currency: str?              (default: USD)
├── line_items: LineItem[]
│   └── LineItem
│       ├── description: str
│       ├── quantity: float?
│       ├── unit_price: float?
│       └── amount: float
├── subtotal: float?
├── tax_amount: float?
├── total_amount: float
└── notes: str?

MatchResult
├── vendor_match: VendorMatch
│   ├── extracted_name: str
│   ├── matched_vendor_id: int?
│   ├── matched_vendor_name: str?
│   ├── confidence: float       (0.0–1.0)
│   └── alternatives: str[]     (up to 3)
├── item_matches: ItemMatch[]
│   └── ItemMatch
│       ├── extracted_description: str
│       ├── matched_item: str?
│       ├── confidence: float   (0.0–1.0)
│       └── alternatives: str[] (up to 3)
└── all_high_confidence: bool

ValidationResult
├── is_valid: bool
└── flags: ValidationFlag[]
    └── ValidationFlag
        ├── item: str
        ├── issue: str          (enum: see table above)
        └── detail: str

ApprovalDecision
├── approved: bool
├── reasoning: str
├── risk_level: str             (low | medium | high)
└── requires_manual_review: bool

StockCheckResult
├── item: str
├── requested_quantity: float
├── available_stock: int
└── shortfall: float
```

### Pipeline State (passed between nodes)

```
PipelineState
├── file_path: str
├── invoice_id: str
├── raw_text: str
├── extracted: ExtractedInvoice?
├── validation: ValidationResult?
├── approval: ApprovalDecision?
├── payment_result: dict?
│   ├── transaction_id: str     (TXN-{hex})
│   ├── status: "completed"
│   ├── amount: float
│   ├── currency: str
│   └── timestamp: str          (ISO 8601)
├── match_result: MatchResult?
├── stock_issues: StockCheckResult[]?
└── approval_attempts: int
```

---

## Database Schema

### Entity-Relationship Diagram

```
┌───────────────┐       ┌───────────────────┐
│   inventory   │       │      vendors      │
├───────────────┤       ├───────────────────┤
│ item     (PK) │       │ id     (PK, auto) │
│ stock    (INT)│       │ name   (UNIQUE)   │
└───────────────┘       │ email             │
                        │ phone             │
                        │ address           │
                        │ created_at        │
                        └───────────────────┘

┌──────────────────────────┐    ┌──────────────────────────────┐
│    processed_invoices    │    │        review_queue          │
├──────────────────────────┤    ├──────────────────────────────┤
│ id          (PK, auto)   │    │ id             (PK, auto)   │
│ invoice_number           │    │ invoice_id     (UNIQUE)     │
│ vendor_name              │    │ review_type                 │
│ filename                 │    │ extracted_json              │
│ total_amount             │    │ match_result_json           │
│ currency     (def: USD)  │    │ validation_json             │
│ transaction_id (UNIQUE)  │    │ stock_issues_json           │
│ line_items_json          │    │ status         (def: pending)│
│ tax_amount               │    │ created_at                  │
│ subtotal                 │    │ resolved_at                 │
│ processed_at             │    └──────────────────────────────┘
│ invoice_date             │
│ due_date                 │
└──────────────────────────┘
```

### Seed Data

**inventory** (4 items)

| item | stock |
|------|-------|
| WidgetA | 15 |
| WidgetB | 10 |
| GadgetX | 5 |
| FakeItem | 0 |

`FakeItem` exists with zero stock specifically to test the out-of-stock rejection path.

**vendors** (14 vendors)

| id | name | Notes |
|----|------|-------|
| 1 | Widgets Inc. | Primary widget supplier |
| 2 | Gadgets Co. | Gadget supplier |
| 3 | Fraudster LLC | Fraud detection test case |
| 4 | Precision Parts Ltd. | |
| 5 | Global Supply Chain Partners | |
| 6 | Acme Industrial Supplies | |
| 7 | MegaWidgets Corp | |
| 8 | NoProd Industries | Unknown items test case |
| 9 | Consolidated Materials Group | |
| 10 | Summit Manufacturing Co. | |
| 11 | QuickShip Distributers | |
| 12 | Atlas Industrial Supply | |
| 13 | TechParts International | |
| 14 | Reliable Components Inc. | |

---

## API Surface

### Batch Processing (Wizard)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/batch/import` | Run import phase (Ingest + Match) for all PENDING invoices |
| POST | `/api/batch/fulfill` | Run fulfill phase (Validate → Pay) for all MATCHED invoices |
| GET | `/api/batch/status` | HTML partial with batch summary counts |

### Single Invoice Processing

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/invoices/{id}/process` | Full end-to-end pipeline run |

### Review Resolution

| Method | Endpoint | Body | Purpose |
|--------|----------|------|---------|
| POST | `/api/invoices/{id}/resolve-match` | `{item_mappings, vendor_name, create_vendor}` | Apply human match corrections |
| POST | `/api/invoices/{id}/reject-match` | — | Reject during match review |
| POST | `/api/invoices/{id}/resolve-stock` | `{action, adjusted_quantities}` | Resolve stock issue (reject/backorder/partial) |

### Data Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/review-queue` | List pending review items (filterable by type) |
| GET | `/api/vendors` | List all vendors |
| POST | `/api/vendors` | Create new vendor |
| GET | `/api/processed-invoices` | List completed invoices |
| POST | `/api/reset-database` | Drop and reseed all tables, reset state |

### SSE Streams

| Endpoint | Events | Purpose |
|----------|--------|---------|
| GET `/sse/invoices/batch/progress?step=1\|3` | `{invoice_id}`, `batch-status`, `keepalive` | Real-time batch progress |
| GET `/sse/invoices/{id}/progress` | `update` | Real-time single invoice detail |

### Page Routes

| Method | Endpoint | Renders |
|--------|----------|---------|
| GET | `/` | Wizard dashboard (all steps) |
| GET | `/invoices/{id}` | Invoice detail page |
| GET | `/settings/vendors` | Vendor management page |

---

## Real-Time Updates

The UI uses **Server-Sent Events (SSE)** with **HTMX** for live progress without a JavaScript framework.

### How It Works

```
 Browser                          Server
    │                               │
    │  GET /sse/.../progress        │
    │──────────────────────────────▶│
    │                               │
    │  ◀── event: INV-1001         │  ◄── Pipeline node completes,
    │      data: <html partial>    │      calls store.update(),
    │                               │      which pushes to subscriber
    │  ◀── event: batch-status     │      queues
    │      data: <html partial>    │
    │                               │
    │  ◀── event: INV-1002         │
    │      data: <html partial>    │
    │                               │
    │  ◀── keepalive (30s)         │
    │      data: ""                │
    │                               │
```

**Key design decisions:**

- **Per-invoice event names:** Each invoice card uses `sse-swap="{invoice_id}"` so it only reacts to its own SSE events. This avoids re-rendering the entire page on each update.
- **Batch status event:** A separate `batch-status` event updates the overall progress summary (counts, wizard step).
- **Step-aware templates:** The SSE endpoint selects the correct row template (`wizard_import_row.html` vs `wizard_fulfill_row.html`) based on the `step` query parameter.

---

## Invoice Test Corpus

17 invoice files across 5 formats, designed to exercise every pipeline path:

### By Scenario

| Scenario | Invoice(s) | Expected Outcome |
|----------|-----------|------------------|
| Clean, valid order | INV-1001 (TXT), INV-1004 (JSON), INV-1006 (CSV), INV-1014 (XML) | COMPLETED |
| OCR errors & typos | INV-1002 (TXT) | Tests extraction agent's error correction |
| Quantity exceeds stock | INV-1002 (GadgetX: 20 requested, 5 available) | NEEDS_STOCK_REVIEW |
| Zero stock / fraud vendor | INV-1003 (Fraudster LLC, FakeItem, $100k, "URGENT") | REJECTED (multiple fraud flags) |
| Unknown items | INV-1008 (SuperGizmo, MegaSprocket), INV-1016 (WidgetC) | NEEDS_MATCH_REVIEW or flagged in validation |
| Negative quantities | INV-1009 (qty = -5, total = -$250, empty vendor) | REJECTED (critical validation) |
| Multi-item with fees | INV-1010 (discounts, shipping, rush fees) | Tests service item handling |
| Non-USD currency | INV-1014 (EUR) | Tests currency preservation |
| PDF extraction | INV-1011, INV-1012, INV-1013 (all PDF) | Tests pdfplumber integration |

### By Format

| Format | Count | Files |
|--------|-------|-------|
| TXT | 7 | 1001, 1002, 1003, 1008, 1010, 1011, 1012 |
| JSON | 6 | 1004, 1004_revised, 1005, 1009, 1013, 1016 |
| CSV | 2 | 1006, 1007 |
| XML | 1 | 1014 |
| PDF | 3 | 1011, 1012, 1013 |

### Complete Pipeline Path Coverage

```
Path 1: Happy path
  Ingest → Match (confident) → Validate (clean) → Approve → Pay → COMPLETED
  Example: INV-1001, INV-1004, INV-1006

Path 2: Match review needed
  Ingest → Match (low confidence) → NEEDS_MATCH_REVIEW → [human] → MATCHED → ...
  Example: INV-1008, INV-1016

Path 3: Stock review needed
  Ingest → Match → Validate (insufficient stock) → NEEDS_STOCK_REVIEW → [human]
  Example: INV-1002

Path 4: Critical validation rejection
  Ingest → Match → Validate (out_of_stock / negative_qty) → REJECTED
  Example: INV-1003, INV-1009

Path 5: Approval rejection (fraud)
  Ingest → Match → Validate (passes) → Approve (fraud detected) → REJECTED
  Example: INV-1003 (if it passes validation, fraud flags catch it)

Path 6: Parse failure
  Ingest (error) → FAILED
  Example: Corrupted or unreadable file
```
