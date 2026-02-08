from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.store import InvoiceStore

router = APIRouter()


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    store = get_store(request)
    env = request.app.state.templates
    invoices = store.get_all()
    html = env.get_template("index.html").render(invoices=invoices)
    return HTMLResponse(html)


@router.get("/invoices/{invoice_id}", response_class=HTMLResponse)
async def invoice_detail(invoice_id: str, request: Request):
    store = get_store(request)
    env = request.app.state.templates
    status = store.get(invoice_id)
    if status is None:
        return HTMLResponse("<h1>Invoice not found</h1>", status_code=404)
    html = env.get_template("detail.html").render(invoice=status)
    return HTMLResponse(html)
