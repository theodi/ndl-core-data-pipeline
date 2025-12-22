import unittest
from datetime import datetime, timezone
import sys
from pathlib import Path

# Ensure src is on sys.path so package imports work when running tests from repo root
repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from ndl_core_data_pipeline.resources.time_utils import parse_to_iso8601_utc


class TestTimeUtils(unittest.TestCase):

    def test_parse_z_suffix(self):
        inp = "2025-01-27T10:26:06Z"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        self.assertEqual(out, "2025-01-27T10:26:06+00:00")

    def test_parse_no_tz(self):
        # naive local time should be treated as UTC by the function
        inp = "2025-01-27T10:26:06"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        self.assertEqual(out, "2025-01-27T10:26:06+00:00")

    def test_parse_with_offset(self):
        # time with explicit offset should be converted to UTC
        inp = "2025-01-27T11:26:06+01:00"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        # 11:26+01:00 -> 10:26+00:00
        self.assertEqual(out, "2025-01-27T10:26:06+00:00")

    def test_parse_space_separator(self):
        # from iso format accepts space as separator too
        inp = "2025-01-27 10:26:06"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        self.assertEqual(out, "2025-01-27T10:26:06+00:00")

    def test_parse_milliseconds_and_z(self):
        inp = "2025-01-27T10:26:06.123Z"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        # Keep milliseconds
        self.assertEqual(out, "2025-01-27T10:26:06.123+00:00")

    def test_simple_date(self):
        inp = "2021-11-01"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        # Keep milliseconds
        self.assertEqual(out, "2021-11-01T00:00:00+00:00")

    def test_parse_english_short_month(self):
        # Accept dates like '1 Mar 2023' and normalize to ISO 8601 UTC
        inp = "1 Mar 2023"
        out = parse_to_iso8601_utc(inp)
        self.assertTrue(out.endswith("+00:00"))
        self.assertEqual(out, "2023-03-01T00:00:00+00:00")

    def test_empty_string(self):
        inp = ""
        out = parse_to_iso8601_utc(inp)
        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()
