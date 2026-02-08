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
