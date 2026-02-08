# Invoice Processing Pipeline — Implementation Plan

## Context

Acme Corp needs an end-to-end invoice processing system that ingests invoices in multiple formats (TXT, JSON, CSV, XML, PDF), extracts structured data via LLM, validates against inventory, runs approval logic, and processes payment. The project has 20 sample invoices with intentional data quality issues (typos, fraud, missing data, stock mismatches). We need a working prototype with a web UI.

**Stack:** PydanticAI (agents + pydantic-graph) · Google Gemini · FastAPI + Jinja2 + HTMX + DaisyUI · SQLite · uv

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  FastAPI Server (server.py)                         │
│  ├─ GET /              → Dashboard (Jinja2 + HTMX)  │
│  ├─ GET /invoices/{id} → Detail view                │
│  ├─ POST /api/process/{id} → Trigger pipeline       │
│  ├─ POST /api/process-all  → Batch process          │
│  └─ GET /sse/progress/{id} → Real-time SSE updates  │
└──────────────┬──────────────────────────────────────┘
               │ BackgroundTask
┌──────────────▼──────────────────────────────────────┐
│  pydantic-graph Pipeline                            │
│  IngestNode → ValidateNode → ApproveNode → PayNode  │
│       │            │              │           │      │
│       ▼            ▼              ▼           ▼      │
│  Gemini LLM    SQLite DB     Gemini LLM   mock_pay  │
└─────────────────────────────────────────────────────┘
```

## Important Gemini Gotcha

Gemini cannot use tools and structured output simultaneously in PydanticAI's default `ToolOutput` mode. For agents that need structured output, use `NativeOutput(MyModel)` wrapper. This frees up tool calling if needed later.

## File Structure

```
galatiq-case-invoices/
├── pyproject.toml              # uv project config
├── .env                        # GEMINI_API_KEY (gitignored)
├── .gitignore
├── main.py                     # CLI entry point
├── server.py                   # FastAPI app entry point
├── app/
│   ├── __init__.py
│   ├── models.py               # Pydantic models (Invoice, LineItem, etc.)
│   ├── agents.py               # PydanticAI agent definitions
│   ├── graph.py                # pydantic-graph pipeline (nodes + graph)
│   ├── database.py             # SQLite setup + queries
│   ├── ingest.py               # File reading/parsing (PDF, CSV, XML, etc.)
│   ├── store.py                # In-memory invoice state + SSE notification
│   ├── api.py                  # FastAPI API routes
│   ├── sse.py                  # SSE streaming endpoints
│   └── pages.py                # HTML page routes (Jinja2)
├── templates/
│   ├── base.html               # Layout (CDN: HTMX, DaisyUI, Tailwind)
│   ├── index.html              # Dashboard grid
│   ├── detail.html             # Single invoice detail
│   └── partials/
│       ├── invoice_card.html   # Card component
│       ├── pipeline_steps.html # Step progress indicator
│       └── results_panel.html  # Extracted data display
├── static/
│   └── style.css               # Minimal custom CSS
├── context/
│   └── plan.md                 # This plan
├── data/
│   ├── invoices/               # (existing sample data)
│   └── generate_pdfs.py        # (existing)
└── inventory.db                # SQLite database (created at startup)
```

## Implementation Steps

### Step 1: Project scaffolding + dependencies

Create `pyproject.toml` with uv, `.env`, `.gitignore`.

**Dependencies:**
- `pydantic-ai-slim[google]` — PydanticAI + Gemini provider
- `pydantic-graph` — state machine orchestration
- `fastapi` — web framework
- `uvicorn[standard]` — ASGI server
- `jinja2` — templating
- `pdfplumber` — PDF text extraction
- `python-dotenv` — env file loading
- `python-multipart` — form uploads

### Step 2: Data models (`app/models.py`)

```python
class LineItem(BaseModel):
    description: str
    quantity: float | None
    unit_price: float | None
    amount: float

class ExtractedInvoice(BaseModel):
    invoice_number: str | None
    vendor_name: str
    invoice_date: str | None
    due_date: str | None
    currency: str | None
    line_items: list[LineItem]
    subtotal: float | None
    tax_amount: float | None
    total_amount: float
    notes: str | None

class ValidationFlag(BaseModel):
    item: str
    issue: str  # "unknown_item", "insufficient_stock", "out_of_stock", "negative_quantity", "data_integrity"
    detail: str

class ValidationResult(BaseModel):
    is_valid: bool
    flags: list[ValidationFlag]

class ApprovalDecision(BaseModel):
    approved: bool
    reasoning: str
    risk_level: str  # "low", "medium", "high"
    requires_manual_review: bool

class PipelineStage(str, Enum):
    PENDING, INGESTING, VALIDATING, APPROVING, PAYING, COMPLETED, REJECTED, FAILED

class InvoiceStatus(BaseModel):
    invoice_id: str
    filename: str
    file_format: str
    stage: PipelineStage
    extracted: ExtractedInvoice | None
    validation: ValidationResult | None
    approval: ApprovalDecision | None
    payment_result: dict | None
    error: str | None
    started_at: datetime | None
    completed_at: datetime | None
```

### Step 3: SQLite database (`app/database.py`)

Create/reset inventory DB at startup with seed data from README:
- WidgetA (15), WidgetB (10), GadgetX (5), FakeItem (0)
- Add helper functions: `check_item_stock(item_name)` → `(exists, stock)`

### Step 4: File ingestion (`app/ingest.py`)

Format-specific readers that return raw text for LLM processing:
- `.txt` → read directly
- `.json` → json.dumps with formatting
- `.csv` → read and format as text table
- `.xml` → parse and format as text
- `.pdf` → pdfplumber text extraction

All return a string that gets passed to the extraction agent.

### Step 5: PydanticAI agents (`app/agents.py`)

Two agents using `google-gla:gemini-2.0-flash`:

1. **Extraction Agent** — takes raw invoice text, returns `ExtractedInvoice`
   - Uses `NativeOutput(ExtractedInvoice)` to avoid Gemini tool/structured-output conflict
   - System prompt instructs handling of typos, OCR errors, missing fields
   - Retries=2 for self-correction

2. **Approval Agent** — takes invoice + validation results, returns `ApprovalDecision`
   - Uses `NativeOutput(ApprovalDecision)`
   - System prompt implements VP-level review rules:
     - Over $10K → high scrutiny, require manual review
     - Validation flags → reason through each, may reject
     - Fraud indicators → reject with explanation
     - Reflection/critique loop via retry + output validator

### Step 6: pydantic-graph pipeline (`app/graph.py`)

State machine with 5 nodes:

```
IngestNode → ValidateNode → ApproveNode → PayNode → End
                 ↓               ↓
            RejectNode ←────────┘
                 ↓
               End
```

- **IngestNode**: Reads file via `ingest.py`, calls extraction agent, stores result in state
- **ValidateNode**: Checks each line item against SQLite inventory, builds validation flags. Routes to ApproveNode (if valid or soft flags) or RejectNode (if critical flags like negative qty)
- **ApproveNode**: Calls approval agent with extracted data + validation results. Routes to PayNode or RejectNode based on decision. Self-loops for reflection (max 2 iterations)
- **PayNode**: Calls `mock_payment()`, returns End with success
- **RejectNode**: Logs rejection reasoning, returns End with rejection

**Graph state** (`PipelineState`): holds raw text, extracted invoice, validation result, approval decision, payment result, agent message history.

**Dependencies** (`PipelineDeps`): db_path, callback function for SSE updates.

### Step 7: In-memory store + SSE (`app/store.py`, `app/sse.py`)

- `InvoiceStore` — dict-based store with asyncio.Queue subscribers for SSE
- Each pipeline stage update pushes to subscribers
- SSE endpoint yields HTML fragments rendered by Jinja2 partials
- Keepalive comments every 30s to prevent timeouts

### Step 8: FastAPI server (`server.py`, `app/api.py`, `app/pages.py`)

- **Pages**: `GET /` (dashboard), `GET /invoices/{id}` (detail)
- **API**: `POST /api/invoices/{id}/process`, `POST /api/invoices/process-all`
- **SSE**: `GET /sse/invoices/{id}/progress`, `GET /sse/invoices/batch/progress`
- Startup event: scan `data/invoices/`, register each file in store, init SQLite DB

### Step 9: Frontend templates

- `base.html` — CDN imports (HTMX 2.x, DaisyUI 5, Tailwind CSS), navbar, layout
- `index.html` — Grid of invoice cards with SSE connection for batch updates
- `detail.html` — Full invoice detail with pipeline progress, extracted data, validation flags, approval reasoning
- Partials: `invoice_card.html`, `pipeline_steps.html`, `results_panel.html` — HTMX-swappable fragments

### Step 10: CLI entry point (`main.py`)

```bash
uv run python main.py --invoice_path=data/invoices/invoice_1001.txt
uv run python main.py --all                    # process all invoices
uv run python server.py                        # start web UI
```

CLI mode runs the pipeline and prints structured results to stdout. Web mode serves the dashboard.

## Invoice Data Summary

The 20 invoices test various scenarios:

| Invoice | Format | Key Test Case |
|---------|--------|---------------|
| INV-1001 | TXT | Clean, should pass all stages |
| INV-1002 | TXT | Typos ("INVOCE", "Vndr"), stock mismatch (20x GadgetX, only 5 available) |
| INV-1003 | TXT | Fraud: "Fraudster LLC", FakeItem (0 stock), "pay yesterday", wire transfer urgency |
| INV-1004 | JSON | Clean, should pass |
| INV-1004_revised | JSON | Duplicate invoice number, different amounts |
| INV-1005 | JSON | Clean, large order ($15K → extra scrutiny) |
| INV-1006 | CSV | Malformed field-value CSV format |
| INV-1007 | CSV | Clean CSV |
| INV-1008 | TXT | Email format, unknown items (SuperGizmo, MegaSprocket) |
| INV-1009 | JSON | Negative quantity (-5), null vendor, null due date |
| INV-1010 | TXT | Rush order with shipping charges |
| INV-1011 | TXT/PDF | Clean |
| INV-1012 | TXT/PDF | OCR errors ("2O26", "Payble", "3,500.O0") |
| INV-1013 | JSON/PDF | Bulk order (8 line items, volume discounts) |
| INV-1014 | XML | EUR currency |
| INV-1015 | CSV | Clean |
| INV-1016 | JSON | Unknown item (WidgetC) |

## Verification

1. **Unit test the pipeline**: `uv run python main.py --invoice_path=data/invoices/invoice_1001.txt` should extract, validate (pass), approve, and pay
2. **Test edge cases**:
   - INV-1002 → stock mismatch flagged (GadgetX: 20 requested, 5 available)
   - INV-1003 → fraud detected, rejected (FakeItem, urgent language)
   - INV-1009 → data integrity error (negative quantity)
   - INV-1008 → unknown items flagged
3. **Web UI**: `uv run python server.py` → open http://localhost:8000 → click "Process All" → watch real-time SSE updates on each card
4. **Batch processing**: All 20 invoices process with appropriate outcomes (some approved/paid, some rejected with reasoning)
