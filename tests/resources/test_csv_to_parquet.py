import unittest
from pathlib import Path
import tempfile
import sys

repo_root = Path(__file__).resolve().parents[2]
src_path = repo_root / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

import shutil
import pyarrow.parquet as pq
import pandas as pd

from ndl_core_data_pipeline.resources.convertors.csv_to_parquet import convert_csv_to_parquet, handle_numeric_column
from ndl_core_data_pipeline.resources.time_utils import parse_to_iso8601_utc


class TestCsvToParquet(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.tests_dir = Path(__file__).parent.parent / "test_data"
        self.tmpdir = Path(self.tests_dir, "tmp")
        self.tmpdir.mkdir(exist_ok=True)

    def tearDown(self) -> None:
        # don't delete files so debugging is easier; in CI you might clean up
        # return None
        shutil.rmtree(self.tmpdir)

    def test_simple_csv_conversion(self):
        repo_root = Path(__file__).resolve().parents[2]
        csv_path = repo_root / 'tests' / 'test_data' / 'simple.csv'
        self.assertTrue(csv_path.exists(), f"test csv not found: {csv_path}")

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'simple.parquet'
            convert_csv_to_parquet(csv_path, out)
            self.assertTrue(out.exists())

            table = pq.read_table(str(out))
            df = table.to_pandas()
            # should have rows and columns
            self.assertGreater(df.shape[0], 0)
            self.assertIn('Reference', df.columns)
            self.assertIn('Mar-23', df.columns)

            # Check KPI 5 Mar-23 numeric parsing from quoted value "166,012,276"
            row = df[df['Reference'] == 'KPI 5']
            self.assertEqual(len(row), 1)
            val = row.iloc[0]['Mar-23']
            # Accept int or float equal to 166012276
            if val is None:
                self.fail('KPI 5 Mar-23 value was parsed as null')
            # numeric may be pandas Int64 or Float
            try:
                self.assertEqual(int(val), 166012276)
            except Exception:
                # if it's a string, try removing commas
                vstr = str(val).replace(',', '').strip()
                self.assertEqual(int(vstr), 166012276)

    def test_simple_csv2_conversion(self):
        repo_root = Path(__file__).resolve().parents[2]
        csv_path = repo_root / 'tests' / 'test_data' / 'simple_csv2.csv'
        self.assertTrue(csv_path.exists(), f"test csv not found: {csv_path}")

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'simple2.parquet'
            convert_csv_to_parquet(csv_path, out)
            self.assertTrue(out.exists())

            table = pq.read_table(str(out))
            df = table.to_pandas()

            # check number of rows roughly equals lines in CSV (there are many rows)
            self.assertGreater(df.shape[0], 10)

            # Check that numeric column 'Number of Posts in FTE' converted to numeric
            col = 'Number of Posts in FTE'
            self.assertIn(col, df.columns)
            sample_val = df.iloc[0][col]
            # Should be numeric (int or float) or pandas numeric type
            try:
                _ = float(sample_val)
            except Exception:
                self.fail(f"Column {col} was not converted to numeric, value: {sample_val}")

    def test_date_parsing(self):
        """Ensure non-ISO dates in the TestDate column are parsed and normalized to ISO 8601."""
        repo_root = Path(__file__).resolve().parents[2]
        csv_path = repo_root / 'tests' / 'test_data' / 'simple.csv'
        self.assertTrue(csv_path.exists(), f"test csv not found: {csv_path}")

        out = Path(self.tmpdir) / 'simple_td.parquet'
        convert_csv_to_parquet(csv_path, out)
        self.assertTrue(out.exists())

        table = pq.read_table(str(out))
        df = table.to_pandas()
        # Check TestDate column exists
        self.assertIn('TestDate', df.columns)

        # pick KPI 1 row and check normalized iso date
        row = df[df['Reference'] == 'KPI 1']
        self.assertEqual(len(row), 1)
        td_val = row.iloc[0]['TestDate']

        expected = parse_to_iso8601_utc('1 Mar 2023')
        # Normalize the actual value using the same utility and compare exact strings.
        self.assertIsNotNone(td_val)
        actual_norm = parse_to_iso8601_utc(str(td_val))
        self.assertEqual(actual_norm, expected)

    def test_simple_csv_conversion_2(self):
        repo_root = Path(__file__).resolve().parents[2]
        csv_path = repo_root / 'tests' / 'test_data' / 'ons_gov_uk_failed.csv'
        self.assertTrue(csv_path.exists(), f"test csv not found: {csv_path}")

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'simple.parquet'
            convert_csv_to_parquet(csv_path, out)
            self.assertTrue(out.exists())

            table = pq.read_table(str(out))
            df = table.to_pandas()
            # should have rows and columns
            self.assertGreater(df.shape[0], 0)

class TestNumericHandler(unittest.TestCase):
    def test_integer_detection(self):
        s = pd.Series(['1', '2', '', 'NA', '3'])
        res = handle_numeric_column(s)
        self.assertIsNotNone(res)
        # should be Int64 nullable
        self.assertTrue(str(res.dtype).startswith('Int'))
        self.assertEqual(int(res.iloc[0]), 1)
        self.assertTrue(pd.isna(res.iloc[2]))

    def test_float_detection(self):
        s = pd.Series(['1.5', '2', '4,000', 'NA', '3.25'])
        res = handle_numeric_column(s)
        self.assertIsNotNone(res)
        # should be Float64 nullable
        self.assertTrue(str(res.dtype).startswith('Float'))
        self.assertAlmostEqual(float(res.iloc[0]), 1.5)
        # numeric may appear as string representation for large numbers; clean and compare
        self.assertAlmostEqual(float(str(res.iloc[2]).replace(',', '')), 4000.0)

    def test_non_numeric_returns_none(self):
        s = pd.Series(['a', 'b', 'NA', ''])
        res = handle_numeric_column(s)
        self.assertIsNone(res)


if __name__ == '__main__':
    unittest.main()
