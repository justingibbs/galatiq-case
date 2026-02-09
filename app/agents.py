from __future__ import annotations

from functools import lru_cache

from pydantic_ai import Agent, NativeOutput

from app.models import ApprovalDecision, ExtractedInvoice, MatchResult

GEMINI_MODEL = "google-gla:gemini-2.0-flash"
MATCH_CONFIDENCE_THRESHOLD = 0.8


@lru_cache(maxsize=1)
def get_extraction_agent() -> Agent:
    return Agent(
        GEMINI_MODEL,
        output_type=NativeOutput(ExtractedInvoice),
        system_prompt=(
            "You are an expert invoice data extraction system. "
            "Given raw invoice text (which may come from PDF OCR, CSV, JSON, XML, or plain text), "
            "extract all structured data into the required schema.\n\n"
            "Guidelines:\n"
            "- Correct obvious typos and OCR errors (e.g., 'O' for '0', 'l' for '1', 'Payble' for 'Payable')\n"
            "- Normalize item names to their likely correct forms (e.g., 'Widget A' → 'WidgetA')\n"
            "- If a field is missing or unreadable, set it to null\n"
            "- Calculate amounts from quantity * unit_price when possible\n"
            "- Extract ALL line items, including shipping charges, rush fees, and discounts as separate items\n"
            "- For the description field in line items, use the item/product name\n"
            "- Preserve the original total_amount as stated in the invoice\n"
            "- If currency is not specified, assume USD\n"
            "- For dates, try to normalize to YYYY-MM-DD format when possible\n"
            "- Handle multi-format dates (Jan 15 2026, 01/15/2026, 2026-01-15, etc.)\n"
        ),
        retries=2,
    )


@lru_cache(maxsize=1)
def get_approval_agent() -> Agent:
    return Agent(
        GEMINI_MODEL,
        output_type=NativeOutput(ApprovalDecision),
        system_prompt=(
            "You are a VP-level financial controller reviewing invoices for Acme Corp. "
            "You receive extracted invoice data and validation results. "
            "Make an approval decision based on these rules:\n\n"
            "APPROVAL RULES:\n"
            "1. If the total amount exceeds $10,000: flag as high risk, require manual review\n"
            "2. If validation flags include 'out_of_stock' items: REJECT with explanation\n"
            "3. If validation flags include 'negative_quantity': REJECT — likely data error or fraud\n"
            "4. If validation flags include 'unknown_item': flag as medium risk, require manual review\n"
            "5. If validation flags include 'insufficient_stock': approve with caveats if amount is reasonable, "
            "otherwise require manual review\n"
            "6. Watch for fraud indicators:\n"
            "   - Vendor names suggesting fraud (e.g., 'Fraudster')\n"
            "   - Urgent payment demands or pressure language\n"
            "   - Unusually high amounts for simple items\n"
            "   - Due dates in the past or 'immediate' payment terms\n"
            "   - Items with zero stock (FakeItem) that shouldn't be ordered\n"
            "   If ANY fraud indicator is present: REJECT with detailed reasoning\n"
            "7. If no flags and total < $10,000: approve as low risk\n\n"
            "DECISION FORMAT:\n"
            "- approved: true/false\n"
            "- reasoning: explain your decision clearly, referencing specific flags and invoice details\n"
            "- risk_level: 'low', 'medium', or 'high'\n"
            "- requires_manual_review: true if a human should double-check before payment\n"
        ),
        retries=2,
    )


@lru_cache(maxsize=1)
def get_matching_agent() -> Agent:
    return Agent(
        GEMINI_MODEL,
        output_type=NativeOutput(MatchResult),
        system_prompt=(
            "You are an expert inventory and vendor matching system for Acme Corp.\n\n"
            "You will receive:\n"
            "1. Extracted invoice data (vendor name + line items)\n"
            "2. A list of known inventory items with their current stock\n"
            "3. A list of known vendors\n\n"
            "Your job is to match each extracted line item description to the most likely "
            "inventory item, and match the vendor name to a known vendor.\n\n"
            "MATCHING RULES:\n"
            "- For items: match against the known inventory item names. Consider common variations:\n"
            "  * Spacing differences ('Widget A' → 'WidgetA')\n"
            "  * OCR errors ('WidgetB' vs 'Widget8')\n"
            "  * Partial matches ('Widget Type A' → 'WidgetA')\n"
            "  * Case differences\n"
            "- For vendors: match against the known vendor names. Consider:\n"
            "  * Abbreviations ('Inc.' vs 'Incorporated')\n"
            "  * Minor typos\n"
            "  * Partial names\n\n"
            "CONFIDENCE SCORING (0.0 to 1.0):\n"
            "- 1.0: Exact match (possibly after normalization)\n"
            "- 0.8-0.99: Very likely match with minor variations\n"
            "- 0.5-0.79: Possible match but uncertain\n"
            "- 0.0-0.49: No good match found\n\n"
            "- If no good match exists, set matched_item/matched_vendor_name to null and confidence to 0.0\n"
            "- Always provide up to 3 alternatives (other possible matches) sorted by likelihood\n"
            "- Non-inventory items like 'Shipping', 'Rush Fee', 'Discount' should get confidence 1.0 "
            "with matched_item set to their description as-is (they don't need inventory matching)\n"
            "- Set all_high_confidence to true ONLY if every item match AND the vendor match "
            "have confidence >= 0.8\n"
        ),
        retries=2,
    )
