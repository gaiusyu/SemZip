import hashlib
import tempfile
import unittest
from pathlib import Path

import run_blocks_pure as runner


class WallBenchmarkRunnerTest(unittest.TestCase):
    def test_split_blocks_records_exact_hashes(self):
        payload = b"a\nbb\nccc\ndddd\neeeee\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "input.log"
            source.write_bytes(payload)
            blocks, source_sha = runner.split_blocks(source, root / "blocks", 2)

            self.assertEqual(3, len(blocks))
            self.assertEqual(hashlib.sha256(payload).hexdigest(), source_sha)
            self.assertEqual([5, 9, 6], [int(block["raw_bytes"]) for block in blocks])
            for block in blocks:
                block_payload = Path(str(block["path"])).read_bytes()
                self.assertEqual(hashlib.sha256(block_payload).hexdigest(), block["raw_sha256"])


if __name__ == "__main__":
    unittest.main()
