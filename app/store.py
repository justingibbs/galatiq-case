from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from app.models import InvoiceStatus, PipelineStage


class InvoiceStore:
    """In-memory invoice state store with SSE subscriber support."""

    def __init__(self) -> None:
        self.invoices: dict[str, InvoiceStatus] = {}
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._batch_subscribers: list[asyncio.Queue] = []

    def register_invoice(self, invoice_id: str, filename: str, file_format: str) -> None:
        self.invoices[invoice_id] = InvoiceStatus(
            invoice_id=invoice_id,
            filename=filename,
            file_format=file_format,
        )

    def get(self, invoice_id: str) -> InvoiceStatus | None:
        return self.invoices.get(invoice_id)

    def get_all(self) -> list[InvoiceStatus]:
        return list(self.invoices.values())

    async def update(self, invoice_id: str, **kwargs) -> None:
        status = self.invoices.get(invoice_id)
        if status is None:
            return
        for key, value in kwargs.items():
            setattr(status, key, value)

        # Notify per-invoice subscribers
        for queue in self._subscribers.get(invoice_id, []):
            await queue.put(status)

        # Notify batch subscribers
        for queue in self._batch_subscribers:
            await queue.put(status)

    def subscribe(self, invoice_id: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(invoice_id, []).append(queue)
        return queue

    def unsubscribe(self, invoice_id: str, queue: asyncio.Queue) -> None:
        subs = self._subscribers.get(invoice_id, [])
        if queue in subs:
            subs.remove(queue)

    def subscribe_batch(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._batch_subscribers.append(queue)
        return queue

    def unsubscribe_batch(self, queue: asyncio.Queue) -> None:
        if queue in self._batch_subscribers:
            self._batch_subscribers.remove(queue)

    def get_wizard_step(self) -> int:
        """Derive the current wizard step (1-4) from collective invoice stages."""
        if not self.invoices:
            return 1
        stages = {s.stage for s in self.invoices.values()}

        # If all are in terminal states and none need review → step 4 (done)
        terminal = {
            PipelineStage.COMPLETED, PipelineStage.REJECTED,
            PipelineStage.FAILED, PipelineStage.BACKORDERED,
        }
        if stages <= terminal:
            return 4

        # If any are in fulfill-phase stages → step 3
        fulfill_stages = {
            PipelineStage.VALIDATING, PipelineStage.APPROVING,
            PipelineStage.PAYING, PipelineStage.NEEDS_STOCK_REVIEW,
        }
        if stages & fulfill_stages:
            return 3

        # If any need match review or are matched → step 2
        match_stages = {PipelineStage.MATCHED, PipelineStage.NEEDS_MATCH_REVIEW}
        if stages & match_stages:
            return 2

        # If any are still processing (ingesting/matching) or pending → step 1
        return 1

    def get_batch_summary(self) -> dict:
        """Return counts by category for the UI."""
        summary = {
            "total": len(self.invoices),
            "pending": 0,
            "processing": 0,
            "matched": 0,
            "needs_match_review": 0,
            "needs_stock_review": 0,
            "completed": 0,
            "rejected": 0,
            "failed": 0,
            "backordered": 0,
        }
        processing_stages = {
            PipelineStage.INGESTING, PipelineStage.MATCHING,
            PipelineStage.VALIDATING, PipelineStage.APPROVING,
            PipelineStage.PAYING,
        }
        for s in self.invoices.values():
            if s.stage == PipelineStage.PENDING:
                summary["pending"] += 1
            elif s.stage in processing_stages:
                summary["processing"] += 1
            elif s.stage == PipelineStage.MATCHED:
                summary["matched"] += 1
            elif s.stage == PipelineStage.NEEDS_MATCH_REVIEW:
                summary["needs_match_review"] += 1
            elif s.stage == PipelineStage.NEEDS_STOCK_REVIEW:
                summary["needs_stock_review"] += 1
            elif s.stage == PipelineStage.COMPLETED:
                summary["completed"] += 1
            elif s.stage == PipelineStage.REJECTED:
                summary["rejected"] += 1
            elif s.stage == PipelineStage.FAILED:
                summary["failed"] += 1
            elif s.stage == PipelineStage.BACKORDERED:
                summary["backordered"] += 1
        return summary

    def reset_all(self) -> None:
        """Reset all invoices back to PENDING state."""
        for status in self.invoices.values():
            status.stage = PipelineStage.PENDING
            status.extracted = None
            status.validation = None
            status.approval = None
            status.payment_result = None
            status.match_result = None
            status.stock_issues = None
            status.error = None
            status.started_at = None
            status.completed_at = None


def scan_invoices(invoices_dir: str | Path = "data/invoices") -> dict[str, tuple[str, str]]:
    """Scan the invoices directory and return {invoice_id: (filename, file_format)}."""
    invoices_dir = Path(invoices_dir)
    results: dict[str, tuple[str, str]] = {}
    if not invoices_dir.exists():
        return results

    for f in sorted(invoices_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in {".txt", ".json", ".csv", ".xml", ".pdf"}:
            # Use the stem as invoice_id (e.g., "invoice_1001")
            invoice_id = f.stem
            results[invoice_id] = (f.name, f.suffix.lstrip(".").upper())
    return results
