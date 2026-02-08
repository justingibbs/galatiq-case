from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request

from app.graph import PipelineState, run_pipeline
from app.models import PipelineStage
from app.store import InvoiceStore

router = APIRouter()

INVOICES_DIR = Path("data/invoices")


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


async def _process_invoice(invoice_id: str, store: InvoiceStore, db_path: str) -> None:
    """Background task to run the pipeline for a single invoice."""
    status = store.get(invoice_id)
    if status is None:
        return

    file_path = INVOICES_DIR / status.filename
    await store.update(invoice_id, stage=PipelineStage.INGESTING, started_at=datetime.now(timezone.utc))

    async def on_stage_change(stage, state):
        kwargs = {"stage": stage}
        if state.extracted:
            kwargs["extracted"] = state.extracted
        if state.validation:
            kwargs["validation"] = state.validation
        if state.approval:
            kwargs["approval"] = state.approval
        if state.payment_result:
            kwargs["payment_result"] = state.payment_result
        await store.update(invoice_id, **kwargs)

    try:
        result = await run_pipeline(
            file_path=str(file_path),
            db_path=db_path,
            on_stage_change=on_stage_change,
        )
        await store.update(
            invoice_id,
            stage=result.stage,
            extracted=result.extracted,
            validation=result.validation,
            approval=result.approval,
            payment_result=result.payment_result,
            error=result.error,
            completed_at=datetime.now(timezone.utc),
        )
    except Exception as e:
        await store.update(
            invoice_id,
            stage=PipelineStage.FAILED,
            error=str(e),
            completed_at=datetime.now(timezone.utc),
        )


@router.post("/api/invoices/{invoice_id}/process")
async def process_invoice(
    invoice_id: str, request: Request, background_tasks: BackgroundTasks
):
    store = get_store(request)
    status = store.get(invoice_id)
    if status is None:
        return {"error": "Invoice not found"}

    # Don't re-process if already running
    if status.stage not in (PipelineStage.PENDING, PipelineStage.FAILED, PipelineStage.REJECTED):
        return {"status": "already processing or completed"}

    # Reset to pending before starting
    await store.update(invoice_id, stage=PipelineStage.PENDING, error=None,
                       extracted=None, validation=None, approval=None,
                       payment_result=None, started_at=None, completed_at=None)

    db_path = request.app.state.db_path
    background_tasks.add_task(_process_invoice, invoice_id, store, db_path)
    return {"status": "started", "invoice_id": invoice_id}


@router.post("/api/invoices/process-all")
async def process_all(request: Request, background_tasks: BackgroundTasks):
    store = get_store(request)
    db_path = request.app.state.db_path
    started = []

    for inv_id, status in store.invoices.items():
        if status.stage in (PipelineStage.PENDING, PipelineStage.FAILED, PipelineStage.REJECTED):
            await store.update(inv_id, stage=PipelineStage.PENDING, error=None,
                               extracted=None, validation=None, approval=None,
                               payment_result=None, started_at=None, completed_at=None)
            background_tasks.add_task(_process_invoice, inv_id, store, db_path)
            started.append(inv_id)

    return {"status": "started", "count": len(started), "invoice_ids": started}
