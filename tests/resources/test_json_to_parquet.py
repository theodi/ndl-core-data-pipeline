import unittest
from pathlib import Path
import shutil
import pyarrow.parquet as pq
import pandas as pd

from ndl_core_data_pipeline.resources.convertors.json_to_parquet import convert_json_to_parquet


class TestJsonToParquet(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.tests_dir = Path(__file__).parent.parent / "test_data"
        self.tmpdir = Path(self.tests_dir, "tmp")
        self.tmpdir.mkdir(exist_ok=True)

    def tearDown(self) -> None:
        # don't delete files so debugging is easier; in CI you might clean up
        # return None
        shutil.rmtree(self.tmpdir)

    def test_simple_json_conversion(self):
        input_json = self.tests_dir / "simple_json.json"
        out_parquet = self.tmpdir / "simple_json.parquet"

        res = convert_json_to_parquet(input_json, out_parquet)
        self.assertTrue(out_parquet.exists())
        # Read parquet back and check expected columns exist
        tbl = pq.read_table(str(out_parquet))
        df = tbl.to_pandas()
        # Basic sanity checks
        self.assertIn("objectIdFieldName", df.columns)
        self.assertIn("spatialReference.wkid", df.columns)
        # Check datatypes: spatialReference.wkid should be integer-like or string nullable
        self.assertTrue(pd.api.types.is_integer_dtype(df["spatialReference.wkid"]))

    def test_big_json_conversion(self):
        input_json = self.tests_dir / "big_json.json"
        out_parquet = self.tmpdir / "big_json.parquet"

        res = convert_json_to_parquet(input_json, out_parquet)
        self.assertTrue(out_parquet.exists())
        tbl = pq.read_table(str(out_parquet))
        df = tbl.to_pandas()
        # Should have at least one row
        self.assertGreaterEqual(len(df.index), 1)

    def test_error_payload_raises(self):
        # create a temp json file containing an error dict
        err_json = self.tmpdir / "err.json"
        err_json.write_text('{"error":{"code":499,"message":"Token Required","details":[]}}')
        out_parquet = self.tmpdir / "err.parquet"
        with self.assertRaises(ValueError) as cm:
            convert_json_to_parquet(err_json, out_parquet)
        self.assertIn("Token Required", str(cm.exception))


if __name__ == "__main__":
    unittest.main()

