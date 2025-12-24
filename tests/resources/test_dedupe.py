import unittest
import tempfile
from pathlib import Path

from ndl_core_data_pipeline.resources.refine.dedupe import deduplicate_folder


class TestDedupe(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="dedupetest_"))

    def tearDown(self):
        # Remove created files/dirs
        for p in self.tmpdir.rglob("*"):
            try:
                if p.is_file():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            except Exception:
                pass
        try:
            self.tmpdir.rmdir()
        except Exception:
            pass

    def _write(self, rel_path: str, content: bytes):
        p = self.tmpdir / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
        return p

    def test_basic_deduplication(self):
        # create files: a.txt, b.txt (same as a), c.txt (different)
        a = self._write("a.txt", b"hello world")
        b = self._write("subdir/b.txt", b"hello world")
        c = self._write("subdir/c.txt", b"different")

        output = self.tmpdir / "out.txt"
        kept = deduplicate_folder(self.tmpdir, max_size_bytes=1024 * 1024, output_file=output)

        # Expect 2 kept files (a and c) - order depends on filesystem walk, so check contents
        lines = [l.strip() for l in output.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertEqual(len(lines), 2)
        # Ensure each path exists and corresponds to one of the files we created
        kept_paths = set(Path(l) for l in lines)
        self.assertTrue(a in kept_paths or b in kept_paths)
        self.assertIn(c, kept_paths)

    def test_ignore_large_file(self):
        # create a large file that exceeds provided max_size
        large = self._write("big.bin", b"x" * 2048)
        small = self._write("small.bin", b"y" * 10)

        output = self.tmpdir / "out2.txt"
        # set max_size to 1 KiB to force big.bin to be ignored
        kept = deduplicate_folder(self.tmpdir, max_size_bytes=1024, output_file=output)

        lines = [l.strip() for l in output.read_text(encoding="utf-8").splitlines() if l.strip()]
        # Only small.bin should be listed
        self.assertEqual(len(lines), 1)
        self.assertEqual(Path(lines[0]).name, "small.bin")


if __name__ == "__main__":
    unittest.main()
