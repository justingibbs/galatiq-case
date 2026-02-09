from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.database import (
    get_all_inventory_items,
    get_all_vendors,
    get_processed_invoices,
    get_review_queue,
    get_review_queue_item,
)
from app.models import ExtractedInvoice, MatchResult, StockCheckResult
from app.store import InvoiceStore

import json

router = APIRouter()


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    store = get_store(request)
    env = request.app.state.templates
    invoices = store.get_all()
    html = env.get_template("index.html").render(invoices=invoices, current_path="/")
    return HTMLResponse(html)


@router.get("/invoices/{invoice_id}", response_class=HTMLResponse)
async def invoice_detail(invoice_id: str, request: Request):
    store = get_store(request)
    env = request.app.state.templates
    db_path = request.app.state.db_path
    status = store.get(invoice_id)
    if status is None:
        return HTMLResponse("<h1>Invoice not found</h1>", status_code=404)

    # For review states, load review queue data for the forms
    queue_item = None
    match_result_data = None
    stock_issues_data = None
    inventory_items = []

    if status.stage.value in ("needs_match_review", "needs_stock_review"):
        queue_item = get_review_queue_item(invoice_id, db_path)
        if queue_item and queue_item.get("match_result_json"):
            match_result_data = MatchResult.model_validate_json(
                queue_item["match_result_json"]
            )
        if queue_item and queue_item.get("stock_issues_json"):
            stock_issues_data = [
                StockCheckResult.model_validate(si)
                for si in json.loads(queue_item["stock_issues_json"])
            ]
        inventory_items = get_all_inventory_items(db_path)

    vendors = get_all_vendors(db_path)

    html = env.get_template("detail.html").render(
        invoice=status,
        queue_item=queue_item,
        match_result_data=match_result_data,
        stock_issues_data=stock_issues_data,
        inventory_items=inventory_items,
        vendors=vendors,
        current_path="/invoices",
    )
    return HTMLResponse(html)


@router.get("/review-queue", response_class=HTMLResponse)
async def review_queue_page(request: Request, type: str | None = None):
    env = request.app.state.templates
    db_path = request.app.state.db_path
    store = get_store(request)
    items = get_review_queue(review_type=type, db_path=db_path)

    # Enrich with store data for display
    enriched = []
    for item in items:
        inv_status = store.get(item["invoice_id"])
        enriched.append({
            **item,
            "filename": inv_status.filename if inv_status else item["invoice_id"],
        })

    html = env.get_template("review_queue.html").render(
        items=enriched, current_filter=type, current_path="/review-queue",
    )
    return HTMLResponse(html)


@router.get("/processed", response_class=HTMLResponse)
async def processed_page(request: Request):
    env = request.app.state.templates
    db_path = request.app.state.db_path
    invoices = get_processed_invoices(db_path)
    html = env.get_template("processed.html").render(
        invoices=invoices, current_path="/processed",
    )
    return HTMLResponse(html)


@router.get("/vendors", response_class=HTMLResponse)
async def vendors_page(request: Request):
    env = request.app.state.templates
    db_path = request.app.state.db_path
    vendors = get_all_vendors(db_path)
    html = env.get_template("vendors.html").render(
        vendors=vendors, current_path="/vendors",
    )
    return HTMLResponse(html)
