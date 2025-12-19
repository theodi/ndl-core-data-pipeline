import unittest
import os

from ndl_core_data_pipeline.resources.pdf_extractor import extract_text_from_pdf


class TestPDFExtractor(unittest.TestCase):
    def setUp(self):
        base = os.path.dirname(__file__)
        self.pdf1 = os.path.abspath(os.path.join(base, '..', 'test_data', '9b0b17ab-4769-4d71-9783-05526794ab01.pdf'))
        self.pdf2 = os.path.abspath(os.path.join(base, '..', 'test_data', '03520554-9b1a-4f68-89d0-6164b385b118.pdf'))

    def test_extract_full_pdf1(self):
        text = extract_text_from_pdf(self.pdf1)
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0, "Expected non-empty text for pdf1")
        # Basic sanity: check for a known word common in government PDFs
        self.assertIn('The National Minimum Wage (NMW) was introduced in 1999', text)

    def test_extract_full_pdf2(self):
        # This PDF may contain both selectable text and images. Ensure extractor returns non-empty text (from text or OCR).
        text = extract_text_from_pdf(self.pdf2)
        print(text)
        print(len(text))
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0, "Expected non-empty text for pdf2 (text or OCR)")
        self.assertIn('Epilepsy12 captures information on the care provided', text)

    def test_extract_specific_pages(self):
        # Try extracting only the first page of pdf1
        text = extract_text_from_pdf(self.pdf1, page_numbers=[0])
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0)

    def test_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            extract_text_from_pdf('nonexistent-file.pdf')


if __name__ == '__main__':
    unittest.main()
