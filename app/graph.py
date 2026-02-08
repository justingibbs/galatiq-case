from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Awaitable

from pydantic_graph import BaseNode, End, Graph, GraphRunContext

from app.agents import get_approval_agent, get_extraction_agent
from app.database import check_item_stock
from app.ingest import read_invoice_file
from app.models import (
    ApprovalDecision,
    ExtractedInvoice,
    PipelineStage,
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
    error: str | None = None


# --- Graph state ---

@dataclass
class PipelineState:
    file_path: str = ""
    raw_text: str = ""
    extracted: ExtractedInvoice | None = None
    validation: ValidationResult | None = None
    approval: ApprovalDecision | None = None
    payment_result: dict | None = None
    approval_attempts: int = 0


# --- Graph dependencies ---

@dataclass
class PipelineDeps:
    db_path: str = "inventory.db"
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None


# --- Nodes ---

@dataclass
class IngestNode(BaseNode[PipelineState, PipelineDeps, PipelineResult]):
    async def run(
        self, ctx: GraphRunContext[PipelineState, PipelineDeps]
    ) -> ValidateNode | End[PipelineResult]:
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

        return ValidateNode()


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

        # Route: critical flags go to rejection, otherwise to approval
        if has_critical:
            return RejectNode(reason="Critical validation failures detected")
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

        # Mock payment processing
        payment = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "status": "completed",
            "amount": ctx.state.extracted.total_amount if ctx.state.extracted else 0,
            "currency": (ctx.state.extracted.currency or "USD") if ctx.state.extracted else "USD",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        ctx.state.payment_result = payment

        return End(PipelineResult(
            stage=PipelineStage.COMPLETED,
            extracted=ctx.state.extracted,
            validation=ctx.state.validation,
            approval=ctx.state.approval,
            payment_result=payment,
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
            error=self.reason,
        ))


# --- Graph definition ---

pipeline_graph = Graph(
    nodes=[IngestNode, ValidateNode, ApproveNode, PayNode, RejectNode],
)


# --- Runner ---

async def run_pipeline(
    file_path: str | Path,
    db_path: str = "inventory.db",
    on_stage_change: Callable[[PipelineStage, PipelineState], Awaitable[None]] | None = None,
) -> PipelineResult:
    """Run the full invoice processing pipeline on a single file."""
    state = PipelineState(file_path=str(file_path))
    deps = PipelineDeps(db_path=db_path, on_stage_change=on_stage_change)
    result = await pipeline_graph.run(IngestNode(), state=state, deps=deps)
    return result.output
