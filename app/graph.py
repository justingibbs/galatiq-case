from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Awaitable

from pydantic_graph import BaseNode, End, Graph, GraphRunContext

from app.agents import (
    MATCH_CONFIDENCE_THRESHOLD,
    get_approval_agent,
    get_extraction_agent,
    get_matching_agent,
)
from app.database import (
    check_item_stock,
    deduct_stock,
    get_all_inventory_items,
    get_all_vendors,
    record_processed_invoice,
    save_review_queue_item,
)
from app.ingest import read_invoice_file
from app.models import (
    ApprovalDecision,
    ExtractedInvoice,
    MatchResult,
    PipelineStage,
    StockCheckResult,
    ValidationFlag,
    ValidationResult,
)

# --- Pipeline result ---

@dataclass
class PipelineResult:
    stage: PipelineStage
    extracted: ExtractedInvoice | None = None
    validation: ValidationResult | None = None
    approval: ApprovalDecision | None = None
    payment_result: dict | None = None
    match_result: MatchResult | None = None
    stock_issues: list[StockCheckResult] | None = None
    error: str | None = None


# --- Graph state ---

@dataclass
class PipelineState:
    file_path: str = ""
    invoice_id: str = ""
    raw_text: str = ""
    extracted: ExtractedInvoice | None = None
    validation: ValidationResult | None = None
    approval: ApprovalDecision | None = None
    payment_result: dict | None = None
    match_result: MatchResult | None = None
    stock_issues: list[StockCheckResult] | None = None
    approval_attempts: int = 0


# --- Graph dependencies ---

@dataclass
class PipelineDeps:
    db_path: str = "inventory.db"
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None
    stop_after_match: bool = False


# --- Nodes ---

@dataclass
class IngestNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> MatchNode | End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.INGESTING, ctx.state)

        try:
            ctx.state.raw_text = await asyncio.to_thread(
                read_invoice_file, ctx.state.file_path
            )
        except Exception as e:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error=f"Ingestion failed: {e}",
            ))

        try:
            result = await get_extraction_agent().run(ctx.state.raw_text)
            ctx.state.extracted = result.output
        except Exception as e:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error=f"Extraction failed: {e}",
            ))

        return MatchNode()


@dataclass
class MatchNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> ValidateNode | End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.MATCHING, ctx.state)

        extracted = ctx.state.extracted
        if extracted is None:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error="No extracted data to match",
            ))

        # Get known items and vendors from DB
        inventory_items = await asyncio.to_thread(
            get_all_inventory_items, ctx.deps.db_path
        )
        vendors = await asyncio.to_thread(get_all_vendors, ctx.deps.db_path)

        # Build prompt for matching agent
        prompt = (
            f"Match the following extracted invoice data against known inventory and vendors.\n\n"
            f"EXTRACTED VENDOR: {extracted.vendor_name}\n\n"
            f"EXTRACTED LINE ITEMS:\n"
        )
        for item in extracted.line_items:
            prompt += f"  - {item.description} (qty={item.quantity}, amount={item.amount})\n"

        prompt += f"\nKNOWN INVENTORY ITEMS:\n"
        for inv in inventory_items:
            prompt += f"  - {inv['item']} (stock: {inv['stock']})\n"

        prompt += f"\nKNOWN VENDORS:\n"
        for v in vendors:
            prompt += f"  - ID={v['id']}: {v['name']}\n"

        try:
            result = await get_matching_agent().run(prompt)
            match_result = result.output
            ctx.state.match_result = match_result
        except Exception as e:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                extracted=extracted,
                error=f"Matching failed: {e}",
            ))

        if match_result.all_high_confidence:
            # Remap extracted item descriptions to matched inventory names
            for i, item_match in enumerate(match_result.item_matches):
                if (
                    item_match.matched_item
                    and item_match.confidence >= MATCH_CONFIDENCE_THRESHOLD
                    and i < len(extracted.line_items)
                ):
                    extracted.line_items[i].description = item_match.matched_item

            if ctx.deps.stop_after_match:
                return End(PipelineResult(
                    stage=PipelineStage.MATCHED,
                    extracted=extracted,
                    match_result=match_result,
                ))
            return ValidateNode()
        else:
            # Save to review queue
            await asyncio.to_thread(
                save_review_queue_item,
                invoice_id=ctx.state.invoice_id,
                review_type="match_review",
                extracted_json=extracted.model_dump_json(),
                match_result_json=match_result.model_dump_json(),
                db_path=ctx.deps.db_path,
            )
            return End(PipelineResult(
                stage=PipelineStage.NEEDS_MATCH_REVIEW,
                extracted=extracted,
                match_result=match_result,
            ))


@dataclass
class ValidateNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> ApproveNode | RejectNode | End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.VALIDATING, ctx.state)

        extracted = ctx.state.extracted
        if extracted is None:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error="No extracted data to validate",
            ))

        flags: list[ValidationFlag] = []
        has_critical = False
        stock_issues: list[StockCheckResult] = []

        for item in extracted.line_items:
            desc = item.description.strip()

            # Check for negative quantities
            if item.quantity is not None and item.quantity < 0:
                flags.append(ValidationFlag(
                    item=desc,
                    issue="negative_quantity",
                    detail=f"Negative quantity ({item.quantity}) detected for {desc}",
                ))
                has_critical = True
                continue

            # Check inventory
            exists, stock = await asyncio.to_thread(
                check_item_stock, desc, ctx.deps.db_path
            )

            if not exists:
                flags.append(ValidationFlag(
                    item=desc,
                    issue="unknown_item",
                    detail=f"Item '{desc}' not found in inventory database",
                ))
            elif stock == 0:
                flags.append(ValidationFlag(
                    item=desc,
                    issue="out_of_stock",
                    detail=f"Item '{desc}' has zero stock (out of stock)",
                ))
                has_critical = True
            elif item.quantity is not None and item.quantity > stock:
                flags.append(ValidationFlag(
                    item=desc,
                    issue="insufficient_stock",
                    detail=f"Requested {item.quantity} of '{desc}' but only {stock} in stock",
                ))
                stock_issues.append(StockCheckResult(
                    item=desc,
                    requested_quantity=item.quantity,
                    available_stock=stock,
                    shortfall=item.quantity - stock,
                ))

        # Check for data integrity issues
        if extracted.total_amount < 0:
            flags.append(ValidationFlag(
                item="total",
                issue="data_integrity",
                detail=f"Negative total amount: {extracted.total_amount}",
            ))
            has_critical = True

        if not extracted.vendor_name or extracted.vendor_name.strip() == "":
            flags.append(ValidationFlag(
                item="vendor",
                issue="data_integrity",
                detail="Missing or empty vendor name",
            ))

        validation = ValidationResult(
            is_valid=len(flags) == 0,
            flags=flags,
        )
        ctx.state.validation = validation

        # Route: critical flags (out_of_stock, negative_qty, data integrity) → reject
        if has_critical:
            return RejectNode(reason="Critical validation failures detected")

        # Route: stock issues (insufficient but not zero) → stock review queue
        if stock_issues:
            ctx.state.stock_issues = stock_issues
            await asyncio.to_thread(
                save_review_queue_item,
                invoice_id=ctx.state.invoice_id,
                review_type="stock_review",
                extracted_json=extracted.model_dump_json(),
                validation_json=validation.model_dump_json(),
                stock_issues_json=json.dumps(
                    [si.model_dump() for si in stock_issues]
                ),
                db_path=ctx.deps.db_path,
            )
            return End(PipelineResult(
                stage=PipelineStage.NEEDS_STOCK_REVIEW,
                extracted=extracted,
                validation=validation,
                match_result=ctx.state.match_result,
                stock_issues=stock_issues,
            ))

        return ApproveNode()


@dataclass
class ApproveNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> PayNode | RejectNode | End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.APPROVING, ctx.state)

        extracted = ctx.state.extracted
        validation = ctx.state.validation
        if extracted is None or validation is None:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error="Missing data for approval",
            ))

        ctx.state.approval_attempts += 1

        prompt = (
            f"Review this invoice for approval:\n\n"
            f"Vendor: {extracted.vendor_name}\n"
            f"Invoice Number: {extracted.invoice_number or 'N/A'}\n"
            f"Date: {extracted.invoice_date or 'N/A'}\n"
            f"Due Date: {extracted.due_date or 'N/A'}\n"
            f"Currency: {extracted.currency or 'USD'}\n"
            f"Total Amount: {extracted.total_amount}\n"
            f"Notes: {extracted.notes or 'None'}\n\n"
            f"Line Items:\n"
        )
        for item in extracted.line_items:
            prompt += f"  - {item.description}: qty={item.quantity}, unit_price={item.unit_price}, amount={item.amount}\n"

        prompt += f"\nValidation Result: {'PASSED' if validation.is_valid else 'FLAGS DETECTED'}\n"
        if validation.flags:
            prompt += "Validation Flags:\n"
            for flag in validation.flags:
                prompt += f"  - [{flag.issue}] {flag.item}: {flag.detail}\n"

        try:
            result = await get_approval_agent().run(prompt)
            decision = result.output
            ctx.state.approval = decision
        except Exception as e:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                extracted=extracted,
                validation=validation,
                error=f"Approval agent failed: {e}",
            ))

        if decision.approved:
            return PayNode()
        else:
            return RejectNode(reason=decision.reasoning)


@dataclass
class PayNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.PAYING, ctx.state)

        extracted = ctx.state.extracted
        if extracted is None:
            return End(PipelineResult(
                stage=PipelineStage.FAILED,
                error="No extracted data for payment",
            ))

        # Deduct stock for each matched inventory item
        for item in extracted.line_items:
            if item.quantity is not None and item.quantity > 0:
                exists, _ = await asyncio.to_thread(
                    check_item_stock, item.description, ctx.deps.db_path
                )
                if exists:
                    await asyncio.to_thread(
                        deduct_stock, item.description, item.quantity, ctx.deps.db_path
                    )

        # Mock payment processing
        payment = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "status": "completed",
            "amount": extracted.total_amount,
            "currency": extracted.currency or "USD",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        ctx.state.payment_result = payment

        # Record processed invoice in DB
        line_items_json = json.dumps(
            [li.model_dump() for li in extracted.line_items]
        )
        filename = Path(ctx.state.file_path).name
        await asyncio.to_thread(
            record_processed_invoice,
            invoice_number=extracted.invoice_number,
            vendor_name=extracted.vendor_name,
            filename=filename,
            total_amount=extracted.total_amount,
            currency=extracted.currency or "USD",
            transaction_id=payment["transaction_id"],
            line_items_json=line_items_json,
            tax_amount=extracted.tax_amount,
            subtotal=extracted.subtotal,
            invoice_date=extracted.invoice_date,
            due_date=extracted.due_date,
            db_path=ctx.deps.db_path,
        )

        return End(PipelineResult(
            stage=PipelineStage.COMPLETED,
            extracted=extracted,
            validation=ctx.state.validation,
            approval=ctx.state.approval,
            payment_result=payment,
            match_result=ctx.state.match_result,
        ))


@dataclass
class RejectNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    reason: str = ""

    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> End[PipelineResult]:
        if ctx.deps.on_stage_change:
            await ctx.deps.on_stage_change(PipelineStage.REJECTED, ctx.state)

        return End(PipelineResult(
            stage=PipelineStage.REJECTED,
            extracted=ctx.state.extracted,
            validation=ctx.state.validation,
            approval=ctx.state.approval,
            match_result=ctx.state.match_result,
            error=self.reason,
        ))


# --- Graph definition ---

pipeline_graph = Graph(
    nodes=[IngestNode, MatchNode, ValidateNode, ApproveNode, PayNode, RejectNode],
)


# --- Runner ---

async def run_pipeline(
    file_path: str | Path,
    db_path: str = "inventory.db",
    invoice_id: str = "",
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Run the full invoice processing pipeline on a single file."""
    state = PipelineState(file_path=str(file_path), invoice_id=invoice_id)
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change)
    result = await pipeline_graph.run(IngestNode(), state=state, deps=deps)
    return result.output


# --- Resume runners ---

async def resume_after_match_review(
    extracted: ExtractedInvoice,
    invoice_id: str,
    file_path: str,
    db_path: str = "inventory.db",
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Resume pipeline from ValidateNode after match review resolution."""
    state = PipelineState(
        file_path=file_path,
        invoice_id=invoice_id,
        extracted=extracted,
    )
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change)
    result = await pipeline_graph.run(ValidateNode(), state=state, deps=deps)
    return result.output


async def run_import_phase(
    file_path: str | Path,
    db_path: str = "inventory.db",
    invoice_id: str = "",
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Run import phase only (Ingest + Match). Stops at MATCHED or NEEDS_MATCH_REVIEW."""
    state = PipelineState(file_path=str(file_path), invoice_id=invoice_id)
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change, stop_after_match=True)
    result = await pipeline_graph.run(IngestNode(), state=state, deps=deps)
    return result.output


async def run_fulfill_phase(
    extracted: ExtractedInvoice,
    invoice_id: str,
    file_path: str,
    db_path: str = "inventory.db",
    match_result: MatchResult | None = None,
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Run fulfill phase (Validate → Approve → Pay) for a MATCHED invoice."""
    state = PipelineState(
        file_path=file_path,
        invoice_id=invoice_id,
        extracted=extracted,
        match_result=match_result,
    )
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change)
    result = await pipeline_graph.run(ValidateNode(), state=state, deps=deps)
    return result.output


async def resume_after_stock_review(
    extracted: ExtractedInvoice,
    validation: ValidationResult,
    invoice_id: str,
    file_path: str,
    db_path: str = "inventory.db",
    match_result: MatchResult | None = None,
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Resume pipeline from ApproveNode after stock review resolution."""
    state = PipelineState(
        file_path=file_path,
        invoice_id=invoice_id,
        extracted=extracted,
        validation=validation,
        match_result=match_result,
    )
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change)
    result = await pipeline_graph.run(ApproveNode(), state=state, deps=deps)
    return result.output
