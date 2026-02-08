from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class LineItem(BaseModel):
    description: str
    quantity: float | None = None
    unit_price: float | None = None
    amount: float


class ExtractedInvoice(BaseModel):
    invoice_number: str | None = None
    vendor_name: str
    invoice_date: str | None = None
    due_date: str | None = None
    currency: str | None = None
    line_items: list[LineItem]
    subtotal: float | None = None
    tax_amount: float | None = None
    total_amount: float
    notes: str | None = None


class ValidationFlag(BaseModel):
    item: str
    issue: str  # "unknown_item", "insufficient_stock", "out_of_stock", "negative_quantity", "data_integrity"
    detail: str


class ValidationResult(BaseModel):
    is_valid: bool
    flags: list[ValidationFlag]


class ApprovalDecision(BaseModel):
    approved: bool
    reasoning: str
    risk_level: str  # "low", "medium", "high"
    requires_manual_review: bool


class PipelineStage(str, Enum):
    PENDING = "pending"
    INGESTING = "ingesting"
    VALIDATING = "validating"
    APPROVING = "approving"
    PAYING = "paying"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"


class InvoiceStatus(BaseModel):
    invoice_id: str
    filename: str
    file_format: str
    stage: PipelineStage = PipelineStage.PENDING
    extracted: ExtractedInvoice | None = None
    validation: ValidationResult | None = None
    approval: ApprovalDecision | None = None
    payment_result: dict | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
