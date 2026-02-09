# Invoice Processing Pipeline

An automated multi-agent system that processes invoices end-to-end: extraction, vendor matching, inventory validation, VP-level approval, and mock payment. Built as a working prototype for Acme Corp's invoice automation case study.

> **Note:** This implementation uses **Google Gemini 2.0 Flash** (`gemini-2.0-flash`) as the LLM backend instead of Grok. The original case brief suggested xAI's Grok, but Gemini was chosen for its strong structured output support and seamless integration with PydanticAI.

## Tech Stack

- **LLM Framework** — [PydanticAI](https://ai.pydantic.dev/) (`pydantic-ai-slim[google]`) with native structured output
- **Orchestration** — [pydantic-graph](https://ai.pydantic.dev/graph/) for pipeline state machine
- **Model** — Google Gemini 2.0 Flash via `google-gla:gemini-2.0-flash`
- **Web** — FastAPI + Jinja2 + HTMX + DaisyUI (Tailwind CSS)
- **Real-time** — Server-Sent Events (SSE) via `sse-starlette`
- **Database** — SQLite (inventory, vendors, processed invoices, review queue)
- **PDF Parsing** — pdfplumber

For a comprehensive technical deep-dive into the architecture, agent decision rubrics, data models, and pipeline design, see **[SYSTEM_DESIGN.md](SYSTEM_DESIGN.md)**.

## Pipeline Flow

```
Ingest → Match → Validate → Approve → Pay → COMPLETED
           ↓         ↓          ↓
    NEEDS_MATCH  NEEDS_STOCK  REJECTED
```

Three AI agents handle different stages:
1. **Extraction Agent** — Parses invoices (TXT, JSON, CSV, XML, PDF) into structured data
2. **Matching Agent** — Fuzzy-matches vendor names and line items against the database
3. **Approval Agent** — Evaluates risk and decides approve/reject with reasoning

Items that need human review (ambiguous vendor matches, stock issues) pause the pipeline and enter a review queue, resumable via the web UI.

## Getting Started

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- A [Google AI API key](https://aistudio.google.com/apikey)

### Setup

```bash
# Clone the repo
git clone <repo-url>
cd galatiq-case-invoices

# Install dependencies
uv sync

# Create a .env file with your API key
echo "GOOGLE_API_KEY=your-key-here" > .env
```

### Run the Web UI

```bash
uv run python server.py
```

Open [http://localhost:8000](http://localhost:8000) in your browser. The dashboard provides a guided wizard to import invoices, resolve review items, and view results.

### Run via CLI

```bash
# Process a single invoice
uv run python main.py --invoice_path=data/invoices/invoice_1001.txt

# Process all invoices
uv run python main.py --all
```

## Test Data

The `data/invoices/` directory contains 17 invoices across 5 formats (TXT, JSON, CSV, XML, PDF) designed to exercise different scenarios:

| Scenario | Example Invoices |
|---|---|
| Clean, valid orders | INV-1001, INV-1004, INV-1006 |
| Quantity exceeds stock | INV-1002 (20x GadgetX, only 5 in stock) |
| Zero-stock / suspicious item | INV-1003 (FakeItem) |
| Unknown items | INV-1008, INV-1016 |
| Invalid data (negative qty) | INV-1009 |

## Project Structure

```
app/
  agents.py      # 3 PydanticAI agents (extraction, matching, approval)
  graph.py       # pydantic-graph pipeline nodes + resume logic
  models.py      # Pydantic models and PipelineStage enum
  database.py    # SQLite schema, seed data, queries
  store.py       # In-memory invoice state + SSE subscribers
  api.py         # REST API endpoints
  pages.py       # HTML page routes
  sse.py         # SSE streaming endpoint
  ingest.py      # File reading / format detection
templates/       # Jinja2 HTML templates (HTMX partials)
static/          # CSS / JS assets
data/invoices/   # Sample invoice files
main.py          # CLI entry point
server.py        # FastAPI web server
```
