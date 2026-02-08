from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from app.api import router as api_router
from app.database import init_db
from app.pages import router as pages_router
from app.sse import router as sse_router
from app.store import InvoiceStore, scan_invoices

DB_PATH = "inventory.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database
    init_db(DB_PATH)
    app.state.db_path = DB_PATH

    # Set up Jinja2 templates
    app.state.templates = Environment(
        loader=FileSystemLoader("templates"),
        autoescape=True,
    )

    # Scan and register invoices
    store = InvoiceStore()
    for invoice_id, (filename, file_format) in scan_invoices().items():
        store.register_invoice(invoice_id, filename, file_format)
    app.state.store = store

    print(f"Registered {len(store.invoices)} invoices")
    yield


app = FastAPI(title="Invoice Processing Pipeline", lifespan=lifespan)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Include routers
app.include_router(api_router)
app.include_router(sse_router)
app.include_router(pages_router)


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
