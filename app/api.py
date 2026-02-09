from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel

from app.database import (
    create_vendor,
    get_all_vendors,
    get_processed_invoices,
    get_review_queue,
    get_review_queue_item,
    reset_database,
    resolve_review_queue_item,
)
from fastapi.responses import HTMLResponse

from app.graph import (
    resume_after_stock_review,
    run_fulfill_phase,
    run_import_phase,
    run_pipeline,
)
from app.models import (
    ExtractedInvoice,
    PipelineStage,
    ValidationResult,
)
from app.store import InvoiceStore

router = APIRouter()

INVOICES_DIR = Path("data/invoices")


def get_store(request: Request) -> InvoiceStore:
    return request.app.state.store


# --- Helpers ---

async def _on_stage_change_factory(invoice_id: str, store: InvoiceStore):
    """Create a stage change callback for a given invoice."""
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
        if state.match_result:
            kwargs["match_result"] = state.match_result
        if state.stock_issues:
            kwargs["stock_issues"] = state.stock_issues
        await store.update(invoice_id, **kwargs)
    return on_stage_change


async def _process_invoice(invoice_id: str, store: InvoiceStore, db_path: str) -> None:
    """Background task to run the pipeline for a single invoice."""
    status = store.get(invoice_id)
    if status is None:
        return

    file_path = INVOICES_DIR / status.filename
    await store.update(invoice_id, stage=PipelineStage.INGESTING, started_at=datetime.now(timezone.utc))

    on_stage_change = await _on_stage_change_factory(invoice_id, store)

    try:
        result = await run_pipeline(
            file_path=str(file_path),
            db_path=db_path,
            invoice_id=invoice_id,
            on_stage_change=on_stage_change,
        )
        await store.update(
            invoice_id,
            stage=result.stage,
            extracted=result.extracted,
            validation=result.validation,
            approval=result.approval,
            payment_result=result.payment_result,
            match_result=result.match_result,
            stock_issues=result.stock_issues,
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


async def _import_invoice(invoice_id: str, store: InvoiceStore, db_path: str) -> None:
    """Background task to run import phase (Ingest + Match) for a single invoice."""
    status = store.get(invoice_id)
    if status is None:
        return

    file_path = INVOICES_DIR / status.filename
    await store.update(invoice_id, stage=PipelineStage.INGESTING, started_at=datetime.now(timezone.utc))

    on_stage_change = await _on_stage_change_factory(invoice_id, store)

    try:
        result = await run_import_phase(
            file_path=str(file_path),
            db_path=db_path,
            invoice_id=invoice_id,
            on_stage_change=on_stage_change,
        )
        await store.update(
            invoice_id,
            stage=result.stage,
            extracted=result.extracted,
            match_result=result.match_result,
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


async def _fulfill_invoice(invoice_id: str, store: InvoiceStore, db_path: str) -> None:
    """Background task to run fulfill phase (Validate → Approve → Pay) for a single invoice."""
    status = store.get(invoice_id)
    if status is None or status.extracted is None:
        return

    file_path = str(INVOICES_DIR / status.filename)
    on_stage_change = await _on_stage_change_factory(invoice_id, store)

    try:
        result = await run_fulfill_phase(
            extracted=status.extracted,
            invoice_id=invoice_id,
            file_path=file_path,
            db_path=db_path,
            match_result=status.match_result,
            on_stage_change=on_stage_change,
        )
        await store.update(
            invoice_id,
            stage=result.stage,
            extracted=result.extracted,
            validation=result.validation,
            approval=result.approval,
            payment_result=result.payment_result,
            match_result=result.match_result,
            stock_issues=result.stock_issues,
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


# --- Process endpoints ---

@router.post("/api/invoices/{invoice_id}/process")
async def process_invoice(
    invoice_id: str, request: Request, background_tasks: BackgroundTasks
):
    store = get_store(request)
    status = store.get(invoice_id)
    if status is None:
        return {"error": "Invoice not found"}

    # Allow reprocessing from terminal/review states
    reprocessable = (
        PipelineStage.PENDING,
        PipelineStage.FAILED,
        PipelineStage.REJECTED,
        PipelineStage.NEEDS_MATCH_REVIEW,
        PipelineStage.NEEDS_STOCK_REVIEW,
    )
    if status.stage not in reprocessable:
        return {"status": "already processing or completed"}

    # Reset to pending before starting
    await store.update(
        invoice_id, stage=PipelineStage.PENDING, error=None,
        extracted=None, validation=None, approval=None,
        payment_result=None, match_result=None, stock_issues=None,
        started_at=None, completed_at=None,
    )

    db_path = request.app.state.db_path
    background_tasks.add_task(_process_invoice, invoice_id, store, db_path)
    return {"status": "started", "invoice_id": invoice_id}


@router.post("/api/invoices/process-all")
async def process_all(request: Request, background_tasks: BackgroundTasks):
    store = get_store(request)
    db_path = request.app.state.db_path
    started = []

    reprocessable = (
        PipelineStage.PENDING,
        PipelineStage.FAILED,
        PipelineStage.REJECTED,
        PipelineStage.NEEDS_MATCH_REVIEW,
        PipelineStage.NEEDS_STOCK_REVIEW,
    )

    for inv_id, status in store.invoices.items():
        if status.stage in reprocessable:
            await store.update(
                inv_id, stage=PipelineStage.PENDING, error=None,
                extracted=None, validation=None, approval=None,
                payment_result=None, match_result=None, stock_issues=None,
                started_at=None, completed_at=None,
            )
            background_tasks.add_task(_process_invoice, inv_id, store, db_path)
            started.append(inv_id)

    return {"status": "started", "count": len(started), "invoice_ids": started}


# --- Batch wizard endpoints ---

@router.post("/api/batch/import")
async def batch_import(request: Request, background_tasks: BackgroundTasks):
    """Run import phase (Ingest + Match) for all PENDING invoices."""
    store = get_store(request)
    db_path = request.app.state.db_path
    started = []

    for inv_id, status in store.invoices.items():
        if status.stage == PipelineStage.PENDING:
            await store.update(
                inv_id, stage=PipelineStage.PENDING, error=None,
                extracted=None, validation=None, approval=None,
                payment_result=None, match_result=None, stock_issues=None,
                started_at=None, completed_at=None,
            )
            background_tasks.add_task(_import_invoice, inv_id, store, db_path)
            started.append(inv_id)

    return {"status": "started", "count": len(started), "invoice_ids": started}


@router.post("/api/batch/fulfill")
async def batch_fulfill(request: Request, background_tasks: BackgroundTasks):
    """Run fulfill phase (Validate → Approve → Pay) for all MATCHED + resolved invoices."""
    store = get_store(request)
    db_path = request.app.state.db_path
    started = []

    fulfillable = {PipelineStage.MATCHED}
    for inv_id, status in store.invoices.items():
        if status.stage in fulfillable and status.extracted is not None:
            background_tasks.add_task(_fulfill_invoice, inv_id, store, db_path)
            started.append(inv_id)

    return {"status": "started", "count": len(started), "invoice_ids": started}


@router.get("/api/batch/status")
async def batch_status(request: Request):
    """Return HTML partial with current batch summary."""
    store = get_store(request)
    env = request.app.state.templates
    summary = store.get_batch_summary()
    wizard_step = store.get_wizard_step()
    html = env.get_template("partials/batch_status.html").render(
        summary=summary, wizard_step=wizard_step,
    )
    return HTMLResponse(html)


# --- Match review resolution ---

class MatchResolveBody(BaseModel):
    item_mappings: dict[str, str]  # {extracted_description: corrected_item_name}
    vendor_name: str
    create_vendor: bool = False


@router.post("/api/invoices/{invoice_id}/resolve-match")
async def resolve_match(
    invoice_id: str, body: MatchResolveBody, request: Request,
):
    store = get_store(request)
    db_path = request.app.state.db_path
    status = store.get(invoice_id)

    if status is None:
        return {"error": "Invoice not found"}
    if status.stage != PipelineStage.NEEDS_MATCH_REVIEW:
        return {"error": "Invoice is not awaiting match review"}

    # Get the stored extracted data
    queue_item = get_review_queue_item(invoice_id, db_path)
    if queue_item is None:
        return {"error": "No review queue item found"}

    extracted = ExtractedInvoice.model_validate_json(queue_item["extracted_json"])

    # Apply item mappings
    for li in extracted.line_items:
        if li.description in body.item_mappings:
            li.description = body.item_mappings[li.description]

    # Apply vendor name
    extracted.vendor_name = body.vendor_name

    # Optionally create vendor
    if body.create_vendor:
        create_vendor(body.vendor_name, db_path)

    # Resolve the queue item
    resolve_review_queue_item(invoice_id, db_path)

    # Set to MATCHED (wizard step 3 will trigger the fulfill phase)
    await store.update(invoice_id, stage=PipelineStage.MATCHED, extracted=extracted, match_result=status.match_result)

    # If called via HTMX, return resolved card partial
    if request.headers.get("HX-Request"):
        env = request.app.state.templates
        updated = store.get(invoice_id)
        html = env.get_template("partials/wizard_resolved_match_card.html").render(invoice=updated)
        return HTMLResponse(html)

    return {"status": "resolved", "invoice_id": invoice_id}


@router.post("/api/invoices/{invoice_id}/reject-match")
async def reject_match(invoice_id: str, request: Request):
    """Reject an invoice during match review (unresolvable item matches)."""
    store = get_store(request)
    db_path = request.app.state.db_path
    status = store.get(invoice_id)

    if status is None:
        return {"error": "Invoice not found"}
    if status.stage != PipelineStage.NEEDS_MATCH_REVIEW:
        return {"error": "Invoice is not awaiting match review"}

    resolve_review_queue_item(invoice_id, db_path)
    await store.update(
        invoice_id,
        stage=PipelineStage.REJECTED,
        error="Rejected during match review: unresolvable item matches",
        completed_at=datetime.now(timezone.utc),
    )

    if request.headers.get("HX-Request"):
        env = request.app.state.templates
        updated = store.get(invoice_id)
        html = env.get_template("partials/wizard_resolved_match_card.html").render(invoice=updated)
        return HTMLResponse(html)

    return {"status": "rejected", "invoice_id": invoice_id}


# --- Stock review resolution ---

class StockResolveBody(BaseModel):
    action: str  # "reject", "partial", "backorder"
    adjusted_quantities: dict[str, float] | None = None  # {item_name: new_qty}


@router.post("/api/invoices/{invoice_id}/resolve-stock")
async def resolve_stock(
    invoice_id: str, body: StockResolveBody, request: Request,
    background_tasks: BackgroundTasks,
):
    store = get_store(request)
    db_path = request.app.state.db_path
    status = store.get(invoice_id)

    if status is None:
        return {"error": "Invoice not found"}
    if status.stage != PipelineStage.NEEDS_STOCK_REVIEW:
        return {"error": "Invoice is not awaiting stock review"}

    queue_item = get_review_queue_item(invoice_id, db_path)
    if queue_item is None:
        return {"error": "No review queue item found"}

    extracted = ExtractedInvoice.model_validate_json(queue_item["extracted_json"])
    validation = ValidationResult.model_validate_json(queue_item["validation_json"])

    # Resolve the queue item
    resolve_review_queue_item(invoice_id, db_path)

    def _htmx_response(inv_id: str):
        if request.headers.get("HX-Request"):
            env = request.app.state.templates
            updated = store.get(inv_id)
            html = env.get_template("partials/wizard_resolved_stock_card.html").render(invoice=updated)
            return HTMLResponse(html)
        return None

    if body.action == "reject":
        await store.update(
            invoice_id,
            stage=PipelineStage.REJECTED,
            error="Rejected during stock review",
            completed_at=datetime.now(timezone.utc),
        )
        htmx = _htmx_response(invoice_id)
        if htmx:
            return htmx
        return {"status": "rejected", "invoice_id": invoice_id}

    if body.action == "backorder":
        await store.update(
            invoice_id,
            stage=PipelineStage.BACKORDERED,
            completed_at=datetime.now(timezone.utc),
        )
        htmx = _htmx_response(invoice_id)
        if htmx:
            return htmx
        return {"status": "backordered", "invoice_id": invoice_id}

    if body.action == "partial":
        # Adjust quantities and recalculate totals
        if body.adjusted_quantities:
            for li in extracted.line_items:
                if li.description in body.adjusted_quantities:
                    new_qty = body.adjusted_quantities[li.description]
                    if li.unit_price is not None:
                        li.amount = new_qty * li.unit_price
                    li.quantity = new_qty

            # Recalculate total
            extracted.total_amount = sum(li.amount for li in extracted.line_items)
            if extracted.tax_amount is not None:
                extracted.subtotal = extracted.total_amount - extracted.tax_amount

        # Remove insufficient_stock flags from validation since we adjusted
        validation = ValidationResult(
            is_valid=True,
            flags=[f for f in validation.flags if f.issue != "insufficient_stock"],
        )

        # Resume from ApproveNode
        file_path = str(INVOICES_DIR / status.filename)

        # Get match_result if available
        mr = status.match_result

        async def _resume():
            on_stage_change = await _on_stage_change_factory(invoice_id, store)
            try:
                result = await resume_after_stock_review(
                    extracted=extracted,
                    validation=validation,
                    invoice_id=invoice_id,
                    file_path=file_path,
                    db_path=db_path,
                    match_result=mr,
                    on_stage_change=on_stage_change,
                )
                await store.update(
                    invoice_id,
                    stage=result.stage,
                    extracted=result.extracted,
                    validation=result.validation,
                    approval=result.approval,
                    payment_result=result.payment_result,
                    match_result=result.match_result,
                    stock_issues=None,
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

        await store.update(
            invoice_id, stage=PipelineStage.APPROVING,
            extracted=extracted, validation=validation, stock_issues=None,
        )
        background_tasks.add_task(_resume)

        htmx = _htmx_response(invoice_id)
        if htmx:
            return htmx
        return {"status": "resumed", "invoice_id": invoice_id}

    return {"error": f"Unknown action: {body.action}"}


# --- Review queue ---

@router.get("/api/review-queue")
async def api_review_queue(request: Request, type: str | None = None):
    db_path = request.app.state.db_path
    items = get_review_queue(review_type=type, db_path=db_path)
    return {"items": items}


# --- Vendors ---

@router.get("/api/vendors")
async def api_vendors(request: Request):
    db_path = request.app.state.db_path
    vendors = get_all_vendors(db_path)
    return {"vendors": vendors}


class CreateVendorBody(BaseModel):
    name: str
    email: str | None = None
    phone: str | None = None
    address: str | None = None


@router.post("/api/vendors")
async def api_create_vendor(body: CreateVendorBody, request: Request):
    db_path = request.app.state.db_path
    vendor_id = create_vendor(
        body.name, db_path, email=body.email, phone=body.phone, address=body.address,
    )
    return {"id": vendor_id, "name": body.name}


# --- Processed invoices ---

@router.get("/api/processed-invoices")
async def api_processed_invoices(request: Request):
    db_path = request.app.state.db_path
    invoices = get_processed_invoices(db_path)
    return {"invoices": invoices}


# --- Database reset ---

@router.post("/api/reset-database")
async def api_reset_database(request: Request):
    db_path = request.app.state.db_path
    store = get_store(request)
    reset_database(db_path)
    store.reset_all()
    return {"status": "reset"}
