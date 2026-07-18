from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

import pare_dataset_extract as dataset_extract


class OpenFunctionStringFallbackTest(unittest.TestCase):
    def test_overflow_fallback_stores_placeholder_payload(self) -> None:
        program = {
            "op": "python_exec",
            "group_regex": r"^(?:audit\(([0-9]+)\.([0-9]+):([0-9]+)\))$",
            "code": (
                "def forward(groups):\n"
                "    value = (int(groups[0]) * 1000000 + int(groups[1])) * 1000000 + int(groups[2])\n"
                "    return {'stored': [value], 'layout': []}\n\n"
                "def inverse(record):\n"
                "    value = int(record['stored'][0])\n"
                "    sequence = value % 1000000\n"
                "    micros = (value // 1000000) % 1000000\n"
                "    epoch = value // 1000000000000\n"
                "    return f'{epoch}.{micros}:{sequence}'"
            ),
        }
        spec = dataset_extract.ExtractSpec(
            tag="TS",
            pattern=r"audit\(([0-9]+)\.([0-9]+):([0-9]+)\)",
            kind="open_function",
            store_group=0,
            replacement="audit({placeholder})",
            context_tag=json.dumps(program, sort_keys=True),
        )
        originals = [
            "audit(1138278091.510:111925)",
            "audit(1138278091.510:111926)",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            metadata = dataset_extract.write_tag_stream(output_dir, spec, originals)
            decoded_values = dataset_extract.read_tag_stream(output_dir, metadata)

        self.assertEqual("string", metadata["kind"])
        self.assertEqual(
            ["1138278091.510:111925", "1138278091.510:111926"],
            decoded_values,
        )
        for original, decoded_value in zip(originals, decoded_values):
            match = re.fullmatch(spec.pattern, original)
            self.assertIsNotNone(match)
            rebuilt = dataset_extract._safe_replacement_format(
                spec.replacement or "{placeholder}",
                decoded_value,
                match,
            )
            self.assertEqual(original, rebuilt)


if __name__ == "__main__":
    unittest.main()
