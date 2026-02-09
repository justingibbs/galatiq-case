from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from app.store import InvoiceStore

router = APIRouter()


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


@router.get("/sse/invoices/batch/progress")
async def batch_progress(request: Request, step: int = 1):
    store = get_store(request)
    templates = request.app.state.templates

    # Choose template based on wizard step
    if step == 3:
        row_template = "partials/wizard_fulfill_row.html"
    else:
        row_template = "partials/wizard_import_row.html"

    async def event_generator():
        queue = store.subscribe_batch()
        try:
            while True:
                try:
                    status = await asyncio.wait_for(queue.get(), timeout=30.0)

                    # Per-invoice row update
                    html = templates.get_template(row_template).render(invoice=status)
                    yield {
                        "event": status.invoice_id,
                        "data": html,
                    }

                    # Batch status summary update
                    summary = store.get_batch_summary()
                    wizard_step = store.get_wizard_step()
                    status_html = templates.get_template("partials/batch_status.html").render(
                        summary=summary, wizard_step=wizard_step,
                    )
                    yield {
                        "event": "batch-status",
                        "data": status_html,
                    }

                except asyncio.TimeoutError:
                    yield {"event": "keepalive", "data": ""}
                except asyncio.CancelledError:
                    break
        finally:
            store.unsubscribe_batch(queue)

    return EventSourceResponse(event_generator())


@router.get("/sse/invoices/{invoice_id}/progress")
async def invoice_progress(invoice_id: str, request: Request):
    store = get_store(request)
    templates = request.app.state.templates

    async def event_generator():
        queue = store.subscribe(invoice_id)
        try:
            while True:
                try:
                    status = await asyncio.wait_for(queue.get(), timeout=30.0)
                    html = templates.get_template("partials/detail_content.html").render(
                        invoice=status
                    )
                    yield {"event": "update", "data": html}
                except asyncio.TimeoutError:
                    yield {"event": "keepalive", "data": ""}
                except asyncio.CancelledError:
                    break
        finally:
            store.unsubscribe(invoice_id, queue)

    return EventSourceResponse(event_generator())
