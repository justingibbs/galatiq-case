"""CLI entry point for the invoice processing pipeline."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from app.database import init_db
from app.graph import run_pipeline
from app.models import PipelineStage


DB_PATH = "inventory.db"
INVOICES_DIR = Path("data/invoices")


async def process_single(invoice_path: str) -> bool:
    """Process a single invoice and print results. Returns True if successful."""
    path = Path(invoice_path)
    if not path.exists():
        print(f"Error: File not found: {path}")
        return False

    print(f"\nProcessing: {path.name}")
    print("=" * 60)

    async def on_stage(stage, state):
        print(f"  Stage: {stage.value}")

    result = await run_pipeline(
        file_path=str(path),
        db_path=DB_PATH,
        on_stage_change=on_stage,
    )

    print(f"\nResult: {result.stage.value}")

    if result.extracted:
        inv = result.extracted
        print(f"\n  Vendor:   {inv.vendor_name}")
        print(f"  Invoice:  {inv.invoice_number or 'N/A'}")
        print(f"  Date:     {inv.invoice_date or 'N/A'}")
        print(f"  Due:      {inv.due_date or 'N/A'}")
        print(f"  Currency: {inv.currency or 'USD'}")
        print(f"  Total:    {inv.total_amount}")
        print(f"  Items:")
        for item in inv.line_items:
            print(f"    - {item.description}: qty={item.quantity}, price={item.unit_price}, amount={item.amount}")

    if result.validation:
        val = result.validation
        if val.is_valid:
            print(f"\n  Validation: PASSED")
        else:
            print(f"\n  Validation: FLAGS DETECTED")
            for flag in val.flags:
                print(f"    [{flag.issue}] {flag.item}: {flag.detail}")

    if result.approval:
        appr = result.approval
        status = "APPROVED" if appr.approved else "REJECTED"
        print(f"\n  Approval: {status} (risk: {appr.risk_level})")
        print(f"  Reasoning: {appr.reasoning}")
        if appr.requires_manual_review:
            print(f"  ** Manual review required **")

    if result.payment_result:
        pay = result.payment_result
        print(f"\n  Payment: {pay['transaction_id']} - {pay['status']}")

    if result.error:
        print(f"\n  Error: {result.error}")

    print()
    return result.stage in (PipelineStage.COMPLETED, PipelineStage.REJECTED)


async def process_all() -> None:
    """Process all invoices in the data/invoices directory."""
    if not INVOICES_DIR.exists():
        print(f"Error: Directory not found: {INVOICES_DIR}")
        return

    files = sorted(
        f for f in INVOICES_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in {".txt", ".json", ".csv", ".xml", ".pdf"}
    )

    print(f"Found {len(files)} invoices to process\n")

    results = {"completed": 0, "rejected": 0, "failed": 0}
    for f in files:
        try:
            success = await process_single(str(f))
            if success:
                results["completed"] += 1
            else:
                results["failed"] += 1
        except Exception as e:
            print(f"  FAILED: {e}\n")
            results["failed"] += 1

    print("=" * 60)
    print(f"Summary: {results['completed']} processed, {results['rejected']} rejected, {results['failed']} failed")


def main():
    parser = argparse.ArgumentParser(description="Invoice Processing Pipeline")
    parser.add_argument("--invoice_path", type=str, help="Path to a single invoice file")
    parser.add_argument("--all", action="store_true", help="Process all invoices")
    args = parser.parse_args()

    # Initialize database
    init_db(DB_PATH)

    if args.invoice_path:
        asyncio.run(process_single(args.invoice_path))
    elif args.all:
        asyncio.run(process_all())
    else:
        print("Usage:")
        print("  uv run python main.py --invoice_path=data/invoices/invoice_1001.txt")
        print("  uv run python main.py --all")
        print("  uv run python server.py  # Start web UI")
        sys.exit(1)


if __name__ == "__main__":
    main()
