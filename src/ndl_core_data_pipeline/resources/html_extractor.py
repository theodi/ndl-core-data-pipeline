"""HTML text extractor utility.

Public functions:
- extract_text_from_html(html: str, url: Optional[str] = None) -> str
- extract_text_from_file(path: Union[str, Path]) -> str

Uses BeautifulSoup (lxml if available) to parse HTML, removes script/style/comments,
extracts visible text, preserves paragraphs and list items as separated lines,
and normalizes whitespace.
"""
from __future__ import annotations

import html as _html
import re
from pathlib import Path
from typing import Optional, Union, Any, cast

try:
    from bs4 import BeautifulSoup, Comment, NavigableString  # type: ignore
    _HAS_BS4 = True
except Exception:
    BeautifulSoup = cast(Any, None)
    Comment = cast(Any, None)
    NavigableString = cast(Any, None)
    _HAS_BS4 = False


def _normalize_whitespace(s: str) -> str:
    # Collapse multiple spaces, normalize newlines: keep paragraph/newline structure
    # First, replace non-breaking spaces
    s = s.replace("\u00A0", " ")
    # Remove trailing/leading whitespace on each line
    lines = [line.strip() for line in s.splitlines()]
    # Collapse multiple blank lines to a single blank line
    out_lines = []
    blank = False
    for line in lines:
        if line == "":
            if not blank:
                out_lines.append("")
            blank = True
        else:
            # collapse internal whitespace
            line = re.sub(r"\s+", " ", line)
            out_lines.append(line)
            blank = False
    # strip leading/trailing blank lines
    while out_lines and out_lines[0] == "":
        out_lines.pop(0)
    while out_lines and out_lines[-1] == "":
        out_lines.pop()
    return "\n".join(out_lines)


def extract_text_from_html(html: str, url: Optional[str] = None) -> str:
    """Extract visible, well-formatted plain text from an HTML string.

    Args:
        html: HTML content (can be full page or fragment).
        url: Optional source URL (unused currently, reserved for future use).

    Returns:
        Plain text with paragraphs and list items separated by newlines. Returns
        empty string for empty/whitespace-only input.

    Raises:
        ValueError: if `html` is not a string.
        RuntimeError: if parsing fails and fallback also fails.
    """
    if not isinstance(html, str):
        raise ValueError("html must be a str")
    if html.strip() == "":
        return ""

    # If BeautifulSoup is available, use it
    if _HAS_BS4:
        # Prefer lxml if available - BeautifulSoup will pick available parser
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        # Remove script/style elements
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # Remove comments
        if Comment is not None:
            for c in soup.find_all(string=lambda text: isinstance(text, Comment)):
                c.extract()

        # Preserve anchor hrefs by appending the URL after the link text
        if NavigableString is not None:
            for a in soup.find_all("a"):
                href = a.get("href")
                if href:
                    # Avoid duplicating if href already appears in the anchor text
                    anchor_text = a.get_text(separator=" ")
                    if href not in anchor_text:
                        try:
                            a.insert_after(NavigableString(f" ({href})"))
                        except Exception:
                            # last-resort: replace inner text to include href
                            a.string = f"{anchor_text} ({href})"

        # Handle block elements: paragraphs, headings, list items
        pieces = []
        for elem in soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "div"]):
            # Skip invisible elements
            text = elem.get_text(separator=" ")
            if text:
                pieces.append(text)

        # If nothing found with block-level scan, fallback to whole-text
        if not pieces:
            whole = soup.get_text(separator=" ")
            text = _html.unescape(whole or "")
            return _normalize_whitespace(text)

        # Join using double newlines for paragraphs and single newlines for lists
        # Heuristic: if element is li -> prefix with '- '
        out_lines = []
        for elem in soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "div"]):
            if elem.name == "li":
                line = elem.get_text(separator=" ")
                line = _html.unescape(line)
                line = re.sub(r"\s+", " ", line).strip()
                out_lines.append("- " + line)
            else:
                line = elem.get_text(separator=" ")
                line = _html.unescape(line)
                line = re.sub(r"\s+", " ", line).strip()
                out_lines.append(line)

        # Combine lines, but place a blank line between consecutive paragraphs/headings
        final_lines = []
        prev_was_para = False
        for line, elem in zip(out_lines, soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "div"])):
            if elem.name in ("p", "h1", "h2", "h3", "h4", "h5", "h6", "div"):
                if prev_was_para and final_lines and final_lines[-1] != "":
                    final_lines.append("")
                final_lines.append(line)
                prev_was_para = True
            else:
                # li
                final_lines.append(line)
                prev_was_para = False

        text = "\n".join(final_lines)
        return _normalize_whitespace(text)

    # Fallback: naive stripping using regex and html.unescape
    try:
        # Remove script/style blocks
        no_script = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
        # Preserve anchor hrefs: replace <a ... href="url">text</a> with 'text (url)'
        no_script = re.sub(r"(?is)<a[^>]*href\s*=\s*[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", r"\2 (\1)", no_script)
        # Preserve list items and paragraphs/newlines heuristically
        # Convert opening li to '- ' and closing li to newline
        no_script = re.sub(r"(?i)<li[^>]*>", "- ", no_script)
        no_script = re.sub(r"(?i)</li>", "\n", no_script)
        # Convert paragraph and br tags to newlines
        no_script = re.sub(r"(?i)<br\s*/?>", "\n", no_script)
        no_script = re.sub(r"(?i)</p>|<p[^>]*>", "\n", no_script)
        # Remove remaining tags
        text = re.sub(r"(?s)<[^>]+>", " ", no_script)
        text = _html.unescape(text)
        return _normalize_whitespace(text)
    except Exception as e:
        raise RuntimeError("Failed to extract text from HTML") from e


def extract_text_from_file(path: Union[str, Path]) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    text = p.read_text(encoding="utf-8")
    return extract_text_from_html(text)
