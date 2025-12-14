import unittest
import os
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient

TEST_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test_data'))


class TestApiClientDownloads(unittest.TestCase):
    def test_download_direct_pdf(self):
        client = RateLimitedApiClient(base_url="https://ckan.publishing.service.gov.uk", rate_limit_per_second=None)
        os.makedirs(TEST_DATA_DIR, exist_ok=True)
        url = "https://www.uttlesford.gov.uk/media/11225/Infrastructure-Funding-Statement-2020-21/pdf/IFS_FINAL_-_Accessible_-_UDC.pdf"
        path, actual_name, fmt = client.download_file(url, TEST_DATA_DIR, preferred_name="3b52225e-f01d-459e-b9ae-ad796348c0de.pdf")
        print(path)
        print(fmt)
        try:
            self.assertIsNotNone(path, "Download returned None path for direct PDF")
            self.assertTrue(os.path.exists(path), f"File was not saved: {path}")
            self.assertIn(fmt.lower() if fmt else None, ("pdf",), f"Expected pdf format, got {fmt}")
            self.assertTrue(path.lower().endswith('.pdf'))
            self.assertEqual("IFS_FINAL_-_Accessible_-_UDC.pdf", actual_name)
        finally:
            # pass
            remove_file(path)

    def test_download_redirected_resource(self):
        client = RateLimitedApiClient(base_url="https://ckan.publishing.service.gov.uk", rate_limit_per_second=None)
        os.makedirs(TEST_DATA_DIR, exist_ok=True)
        # ArcGIS redirected resource example
        url = "https://ntcouncil.maps.arcgis.com/sharing/rest/content/items/aa6fc4f45c5448ee95ce8fe166112fff/data"
        path, actual_name, fmt = client.download_file(url, TEST_DATA_DIR, preferred_name="e7ef30f1-acb1-4052-9270-0a8f65174c7b.csv")
        try:
            self.assertIsNotNone(path, "Download returned None path for redirected resource")
            self.assertTrue(os.path.exists(path), f"File was not saved: {path}")
            self.assertIn((fmt or '').lower(), ("csv", "txt"), f"Expected csv-like format, got {fmt}")
            self.assertTrue(path.lower().endswith('.csv') or (fmt and fmt.lower() == 'csv'))
            self.assertEqual("North_Tyneside_Article4_Direction_Area.csv", actual_name)
        finally:
            # pass
            remove_file(path)

def remove_file(path: str | None):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


if __name__ == '__main__':
    unittest.main()
