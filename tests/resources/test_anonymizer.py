import unittest
from ndl_core_data_pipeline.resources.refine.anonymizer import anonymize_text_presidio


class TestAnonymizer(unittest.TestCase):
    def test_email_redaction(self):
        s = "Contact: alice@example.com for info."
        out = anonymize_text_presidio(s)
        self.assertIn("xxx@xxx.xx", out)
        self.assertNotIn("alice@example.com", out)

    def test_uk_mobile_redaction(self):
        s = "Call me on 07123 456 789 or at +44 7123 456789"
        out = anonymize_text_presidio(s)
        # Should replace both formats
        self.assertNotIn("07123", out)
        self.assertNotIn("+44 7123", out)
        self.assertIn("xx-xxxx-xxxx", out)

    def test_empty_string(self):
        self.assertEqual(anonymize_text_presidio(""), "")

    def test_no_pii(self):
        s = "This text has no contacts."
        out = anonymize_text_presidio(s)
        self.assertEqual(out, s)


if __name__ == "__main__":
    unittest.main()

