from __future__ import annotations

import csv
import io
import json
from pathlib import Path


def read_invoice_file(path: str | Path) -> str:
    """Read an invoice file and return its contents as a string suitable for LLM extraction."""
    path = Path(path)
    suffix = path.suffix.lower()

    readers = {
        ".txt": _read_txt,
        ".json": _read_json,
        ".csv": _read_csv,
        ".xml": _read_xml,
        ".pdf": _read_pdf,
    }

    reader = readers.get(suffix)
    if reader is None:
        raise ValueError(f"Unsupported file format: {suffix}")
    return reader(path)


def _read_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_json(path: Path) -> str:
    data = json.loads(path.read_text(encoding="utf-8"))
    return json.dumps(data, indent=2)


def _read_csv(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return text

    # Format as aligned text table
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*rows)]
    lines = []
    for row in rows:
        padded = [str(cell).ljust(w) for cell, w in zip(row, col_widths)]
        lines.append("  ".join(padded))
    return "\n".join(lines)


def _read_xml(path: Path) -> str:
    # Return the raw XML — it's already structured text the LLM can parse
    return path.read_text(encoding="utf-8")


def _read_pdf(path: Path) -> str:
    import pdfplumber

    texts: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                texts.append(page_text)
    if not texts:
        raise ValueError(f"Could not extract text from PDF: {path}")
    return "\n\n".join(texts)
