#!/usr/bin/env python3
import sys
from pathlib import Path

try:
    from PyPDF2 import PdfReader
except Exception as e:
    print("ERROR: PyPDF2 not installed. Please run: pip install PyPDF2", file=sys.stderr)
    sys.exit(2)

if len(sys.argv) < 2:
    print("Usage: extract_pdfs.py <pdf-file> [more-pdf-files]")
    sys.exit(1)

for p in sys.argv[1:]:
    fp = Path(p)
    if not fp.exists():
        print(f"\n--- File not found: {p}\n")
        continue
    print(f"\n=== Begin: {p} ===\n")
    try:
        reader = PdfReader(str(fp))
        text_parts = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
        text = "\n\n".join(text_parts)
        if not text.strip():
            print("(No extractable text found or PDF is scanned images.)")
        else:
            print(text)
    except Exception as e:
        print(f"Error reading {p}: {e}")
    print(f"\n=== End: {p} ===\n")
