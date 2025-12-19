"""
Simple PDF text extractor using PyMuPDF (fitz) with OCR (pytesseract) fallback.
Provides a single function `extract_text_from_pdf(path, page_numbers=None, ocr_threshold=100, ocr_lang='eng')`.
"""
from typing import Iterable, Optional
import os

# Runs OCR if extracted text length is below this threshold
OCR_THRESHOLD = 200


def _perform_ocr_on_pdf(path: str, page_numbers: Optional[Iterable[int]] = None, lang: str = 'eng') -> str:
    """Perform OCR on the given PDF using pdf2image and pytesseract.

    Note: Requires poppler (e.g. `brew install poppler`) and tesseract (`brew install tesseract`).
    """
    print("Performing OCR on {}".format(path))
    try:
        from pdf2image import convert_from_path
        import pytesseract
        from PIL import Image
    except Exception as e:
        raise RuntimeError("pdf2image, pytesseract and Pillow are required for OCR fallback. Install with 'uv add pdf2image pytesseract Pillow' and ensure poppler is installed on the system.") from e

    # convert pages to images
    pages = None
    if page_numbers is None:
        images = convert_from_path(path)
    else:
        # pdf2image uses 1-based page numbers
        pages = [p + 1 for p in page_numbers]
        images = convert_from_path(path, first_page=min(pages), last_page=max(pages))

    # If a subset was requested but convert_from_path returned more pages (range), pick only requested ones
    if page_numbers is not None and pages is not None:
        # map page indices to images (convert_from_path returns a list starting at first_page)
        first = min(pages)
        selected = []
        for p in pages:
            idx = p - first
            if 0 <= idx < len(images):
                selected.append(images[idx])
        images = selected

    parts = []
    for img in images:
        text = pytesseract.image_to_string(img, lang=lang)
        parts.append(text)

    return "\n".join(parts).strip()


def extract_text_from_pdf(path: str, page_numbers: Optional[Iterable[int]] = None, ocr_threshold: int = OCR_THRESHOLD, ocr_lang: str = 'eng') -> str:
    """Extracts plain text from a PDF file using PyMuPDF (fitz).

    If extracted text length is less than `ocr_threshold`, performs OCR fallback using pytesseract/pdf2image.

    Args:
        path: Path to the PDF file.
        page_numbers: Optional iterable of 0-based page indices to extract. If None, all pages are extracted.
        ocr_threshold: If extracted text length < threshold, OCR is attempted.
        ocr_lang: Language code for Tesseract (default 'eng').

    Returns:
        A string containing the concatenated text from the requested pages (empty string if no text found).

    Raises:
        FileNotFoundError: If the given path does not exist.
        RuntimeError: If PyMuPDF (fitz) cannot open the file for any other reason.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF file not found: {path}")

    try:
        import fitz  # PyMuPDF
    except Exception as e:
        raise RuntimeError("PyMuPDF (fitz) is required to extract PDF text. Install with 'uv add pymupdf'.") from e

    doc = fitz.open(path)
    try:
        total_pages = doc.page_count
        if page_numbers is None:
            pages = range(total_pages)
        else:
            # Normalize and clamp page indices
            pages = [p for p in page_numbers if 0 <= p < total_pages]

        parts = []
        for p in pages:
            page = doc.load_page(p)
            # Use the plain text extraction method
            parts.append(page.get_text("text"))

        text = "\n".join(parts).strip()

        # If text is very short, try OCR fallback
        if len(text) < ocr_threshold:
            ocr_text = _perform_ocr_on_pdf(path, page_numbers=pages if len(list(pages)) > 0 else None, lang=ocr_lang)
            # Prefer OCR text if it's longer
            if len(ocr_text) > len(text):
                return text + "\n\n" + ocr_text
        return text
    finally:
        doc.close()
