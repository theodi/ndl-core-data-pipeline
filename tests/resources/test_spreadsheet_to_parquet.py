import unittest
from pathlib import Path
import pandas as pd
import re
import shutil

from ndl_core_data_pipeline.resources.convertors.spreadsheet_to_parquet import (
    convert_spreadsheet_to_parquet,
)

class TestSpreadsheetToParquet(unittest.TestCase):
    def setUp(self):
        self.test_data = Path(__file__).parent.parent / "test_data"
        self.tmpdir = Path(self.test_data, "tmp")
        self.tmpdir.mkdir(exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _read_parquet(self, p: Path) -> pd.DataFrame:
        return pd.read_parquet(p)

    def test_single_sheet_xlsx(self):
        src = self.test_data / "single_sheet.xlsx"
        out = self.tmpdir / "single.parquet"
        print(out)
        result = convert_spreadsheet_to_parquet(src, out)
        # result should point to a parquet file
        self.assertTrue(result.exists())
        df = self._read_parquet(result)
        # expect at least one column and some rows
        self.assertGreaterEqual(len(df.columns), 1)
        self.assertGreaterEqual(len(df), 1)

    def test_multiple_sheets_xlsx(self):
        src = self.test_data / "multiple_sheets.xlsx"
        outdir = self.tmpdir / "multi_out"
        result = convert_spreadsheet_to_parquet(src, outdir)
        # result should be the directory
        self.assertTrue(result.exists())
        # expect multiple parquet files inside
        files = list(result.glob("*.parquet"))
        self.assertEqual(len(files), 3)
        # read one file and assert basic structure
        df = self._read_parquet(files[0])
        self.assertGreaterEqual(len(df.columns), 1)

        # ensure the 'Water Level Readings' sheet's 'Time' column
        # is NOT converted to ISO 8601. The sheet filename is sanitised to
        # 'Water_Level_Readings.parquet' by the converter.
        water_file = None
        for f in files:
            if f.stem == "Water_Level_Readings" or f.name == "Water_Level_Readings.parquet":
                water_file = f
                break

        self.assertIsNotNone(water_file, "Could not find Water_Level_Readings.parquet in output")
        wf_df = self._read_parquet(water_file)
        # Ensure Time column exists
        self.assertIn("Time", wf_df.columns)
        # Find first non-null value in Time column
        time_series = wf_df["Time"].dropna().astype(str)
        self.assertGreaterEqual(len(time_series), 1, "No non-null values found in 'Time' column")
        first_time_val = time_series.iloc[0].strip()

        # ISO 8601 basic regex (DATE or DATETIME). We expect Time NOT to match this.
        iso_re = re.compile(r'^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?)?$')
        self.assertFalse(bool(iso_re.match(first_time_val)), f"Expected 'Time' value not to be ISO8601 formatted but got '{first_time_val}'")

        self.assertIn("datetime", wf_df.columns)
        datetime_series = wf_df["datetime"].dropna().astype(str)
        self.assertGreaterEqual(len(time_series), 1, "No non-null values found in 'datetime' column")
        first_datetime_val = datetime_series.iloc[0].strip()

        # ISO 8601 basic regex (DATE or DATETIME). We expect datetime to match this.
        iso_re = re.compile(r'^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?)?$')
        self.assertTrue(bool(iso_re.match(first_datetime_val)),
                         f"Expected 'datetime' value to be ISO8601 formatted but got '{first_datetime_val}'")

    def test_multiple_sheets_complex_xlsx(self):
        src = self.test_data / "multiple_sheets_complex.xlsx"
        outdir = self.tmpdir / "multi_complex_out"
        result = convert_spreadsheet_to_parquet(src, outdir)
        self.assertTrue(result.exists())
        files = list(result.glob("*.parquet"))
        # complex workbook should have at least 2 sheets
        self.assertGreaterEqual(len(files), 2)

    def test_ods_file(self):
        src = self.test_data / "1d23678b-a09d-4e75-9093-3eea98a44ee5.ods"
        out = self.tmpdir / "ods_out"
        result = convert_spreadsheet_to_parquet(src, out)
        # If multiple sheets exist, result is a dir
        self.assertTrue(result.exists())
        print(result)
        self.assertTrue(result.is_file())


if __name__ == "__main__":
    unittest.main()
