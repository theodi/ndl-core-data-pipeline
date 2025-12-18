import unittest
import json
from pathlib import Path

from ndl_core_data_pipeline.resources.html_extractor import extract_text_from_html, extract_text_from_file


# Determine path to test_data relative to repository layout
TEST_DATA_DIR = Path(__file__).parent.parent / "test_data"


def _load_text_from_json(file_path: Path) -> str:
    data = json.loads(file_path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "text" in data:
        return data["text"]
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "text" in item:
                return item["text"]
    for v in data.values() if isinstance(data, dict) else []:
        if isinstance(v, str) and "<" in v and ">" in v:
            return v
    return ""


class TestHtmlExtractor(unittest.TestCase):
    def test_extracts_from_gov_uk_sample(self):
        src = Path(__file__).parent.parent / "test_data" / "gov_uk.json"
        html = _load_text_from_json(src)
        self.assertTrue(html, "gov_uk sample 'text' field should be present")

        out = extract_text_from_html(html)
        self.assertNotIn("<", out)
        self.assertNotIn(">", out)
        self.assertNotIn("script", out.lower())
        self.assertNotIn("style", out.lower())
        self.assertTrue("AAIB" in out or "Air Accidents Investigation Branch" in out or "Investigation" in out)
        self.assertNotIn("  ", out)
        self.assertEqual(out.strip(), out)
        # ensure hrefs are preserved as text in parentheses
        self.assertTrue("https://assets.publishing.service.gov.uk/media/68e38e58750fcf90fa6ffdfc/Short_Bros_SD3-60_N915GD_10-25.pdf" in out or "https://assets.publishing.service.gov.uk/media/68e38de0c487360cc70ca12d/Abbreviations.pdf" in out)
        print(out)

    def test_extracts_from_legislation_sample(self):
        src = Path(__file__).parent.parent / "test_data" / "legislation.json"
        html = _load_text_from_json(src)
        self.assertTrue(html, "legislation sample 'text' field should be present")

        out = extract_text_from_html(html)
        self.assertNotIn("<", out)
        self.assertNotIn(">", out)
        self.assertTrue(any(k in out for k in ["Section", "Act", "Part"]) or len(out.split()) > 50)
        print(out)

    def test_empty_and_file_function(self):
        self.assertEqual(extract_text_from_html(""), "")
        p = Path("tests/tmp_test_html.html")
        p.write_text("<html><body><p>Hello<b>World</b></p></body></html>", encoding="utf-8")
        try:
            out = extract_text_from_file(p)
            self.assertIn("Hello", out)
            self.assertIn("World", out)
        finally:
            p.unlink()

    def test_no_tags_and_normalized(self):
        html = "<html><head><style>.a{}</style><script>var a=1;</script></head><body><h1>Title</h1><p>First    line</p><p>Second\nline</p><ul><li>One</li><li>Two</li></ul></body></html>"
        out = extract_text_from_html(html)
        self.assertIn("Title", out)
        self.assertIn("First line", out)
        self.assertTrue("Second line" in out or "Second\nline" in out)
        # use inline (?m) flag so ^ matches at line starts
        self.assertRegex(out, r"(?m)^-\s+One", msg=f"out was: {out}")
        self.assertRegex(out, r"(?m)^-\s+Two", msg=f"out was: {out}")


if __name__ == "__main__":
    unittest.main()
