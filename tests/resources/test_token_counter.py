try:
    import tiktoken  # type: ignore
    HAS_TIKTOKEN = True
except Exception:
    HAS_TIKTOKEN = False

import unittest
from ndl_core_data_pipeline.resources.token_counter import count_tokens


@unittest.skipUnless(HAS_TIKTOKEN, "tiktoken is not installed; skipping token counter tests")
class TestTokenCounter(unittest.TestCase):
    def test_empty_and_none(self):
        self.assertEqual(count_tokens(""), 0)
        self.assertEqual(count_tokens(None), 0)

    def test_basic_english(self):
        text = "Hello, world!"
        cnt = count_tokens(text)
        self.assertIsInstance(cnt, int)
        self.assertEqual(cnt, 4)

    def test_consistency(self):
        text = "The quick brown fox jumps over the lazy dog."
        a = count_tokens(text)
        b = count_tokens(text)
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
