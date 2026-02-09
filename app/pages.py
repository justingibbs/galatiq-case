from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.database import (
    get_all_inventory_items,
    get_all_vendors,
    get_review_queue_item,
)
from app.models import MatchResult, StockCheckResult
from app.store import InvoiceStore

router = APIRouter()


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


@router.get("/", response_class=HTMLResponse)
async def wizard_page(request: Request, step: int | None = None):
    store = get_store(request)
    env = request.app.state.templates
    db_path = request.app.state.db_path

    wizard_step = step if step is not None else store.get_wizard_step()
    summary = store.get_batch_summary()
    invoices = store.get_all()

    # For step 2, gather match review data keyed by invoice_id
    match_review_map = {}
    if wizard_step == 2:
        vendors = get_all_vendors(db_path)
        inventory_items = get_all_inventory_items(db_path)
        for inv in invoices:
            if inv.stage.value == "needs_match_review":
                queue_item = get_review_queue_item(inv.invoice_id, db_path)
                if queue_item and queue_item.get("match_result_json"):
                    match_review_map[inv.invoice_id] = {
                        "match_result": MatchResult.model_validate_json(
                            queue_item["match_result_json"]
                        ),
                        "vendors": vendors,
                        "inventory_items": inventory_items,
                    }

    # For step 3, gather stock review data keyed by invoice_id
    stock_review_map = {}
    if wizard_step == 3:
        for inv in invoices:
            if inv.stage.value == "needs_stock_review":
                queue_item = get_review_queue_item(inv.invoice_id, db_path)
                if queue_item and queue_item.get("stock_issues_json"):
                    stock_review_map[inv.invoice_id] = [
                        StockCheckResult.model_validate(si)
                        for si in json.loads(queue_item["stock_issues_json"])
                    ]

    html = env.get_template("wizard.html").render(
        invoices=invoices,
        wizard_step=wizard_step,
        summary=summary,
        match_review_map=match_review_map,
        stock_review_map=stock_review_map,
    )
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
    wizard_step = store.get_wizard_step()

    html = env.get_template("detail.html").render(
        invoice=status,
        queue_item=queue_item,
        match_result_data=match_result_data,
        stock_issues_data=stock_issues_data,
        inventory_items=inventory_items,
        vendors=vendors,
        wizard_step=wizard_step,
    )
    return HTMLResponse(html)


@router.get("/settings/vendors", response_class=HTMLResponse)
async def vendors_page(request: Request):
    env = request.app.state.templates
    db_path = request.app.state.db_path
    vendors = get_all_vendors(db_path)
    html = env.get_template("vendors.html").render(
        vendors=vendors,
    )
    return HTMLResponse(html)
