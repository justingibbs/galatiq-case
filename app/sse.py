from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from app.store import InvoiceStore

router = APIRouter()


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


@router.get("/sse/invoices/batch/progress")
async def batch_progress(request: Request):
    store = get_store(request)
    templates = request.app.state.templates

    async def event_generator():
        queue = store.subscribe_batch()
        try:
            while True:
                try:
                    status = await asyncio.wait_for(queue.get(), timeout=30.0)
                    html = templates.get_template("partials/invoice_card.html").render(
                        invoice=status
                    )
                    yield {
                        "event": status.invoice_id,
                        "data": html,
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
