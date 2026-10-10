"""Check that application code stays free of decorative comments and emoji logs."""

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "database_pipeline_py"
DECORATIVE = re.compile(r"^\s*#\s*[─═-]{5,}|^\s*#\s*─+.*─+")
EMOJI = re.compile(r"[✅🚀📊⚠️🎯💡]")


class SourceStyleTests(unittest.TestCase):
    def test_no_decorative_comment_banners_or_emoji(self):
        for path in ROOT.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=str(path)):
                self.assertFalse(EMOJI.search(source), str(path))
                self.assertFalse(
                    any(DECORATIVE.match(line) for line in source.splitlines()),
                    str(path),
                )


if __name__ == "__main__":
    unittest.main()
