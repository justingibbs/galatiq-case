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


# --- Matching models ---

class ItemMatch(BaseModel):
    extracted_description: str
    matched_item: str | None = None
    confidence: float  # 0.0 - 1.0
    alternatives: list[str] = []


class VendorMatch(BaseModel):
    extracted_name: str
    matched_vendor_id: int | None = None
    matched_vendor_name: str | None = None
    confidence: float
    alternatives: list[str] = []


class MatchResult(BaseModel):
    vendor_match: VendorMatch
    item_matches: list[ItemMatch]
    all_high_confidence: bool


# --- Stock check models ---

class StockCheckResult(BaseModel):
    item: str
    requested_quantity: float
    available_stock: int
    shortfall: float


# --- Pipeline enums & status ---

class PipelineStage(str, Enum):
    PENDING = "pending"
    INGESTING = "ingesting"
    MATCHING = "matching"
    VALIDATING = "validating"
    APPROVING = "approving"
    PAYING = "paying"
    MATCHED = "matched"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"
    NEEDS_MATCH_REVIEW = "needs_match_review"
    NEEDS_STOCK_REVIEW = "needs_stock_review"
    BACKORDERED = "backordered"


class InvoiceStatus(BaseModel):
    invoice_id: str
    filename: str
    file_format: str
    stage: PipelineStage = PipelineStage.PENDING
    extracted: ExtractedInvoice | None = None
    validation: ValidationResult | None = None
    approval: ApprovalDecision | None = None
    payment_result: dict | None = None
    match_result: MatchResult | None = None
    stock_issues: list[StockCheckResult] | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
