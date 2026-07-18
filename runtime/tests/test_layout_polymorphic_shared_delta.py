from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

import pare_dataset_extract as dataset_extract
import semzip_pure


class LayoutPolymorphicSharedDeltaTest(unittest.TestCase):
    def health_timestamp_program(self) -> dict[str, object]:
        return {
            "op": "python_exec",
            "group_regex": r"^(\d{8})-(\d{2}:\d{2}:\d{2}):(\d{3})$",
            "code": (
                "def forward(groups):\n"
                "    dt = datetime.strptime(groups[0] + ' ' + groups[1], '%Y%m%d %H:%M:%S')\n"
                "    return {'stored': [int(dt.timestamp() * 1000) + int(groups[2])], 'layout': []}\n\n"
                "def inverse(record):\n"
                "    value = int(record['stored'][0])\n"
                "    dt = datetime.fromtimestamp(value // 1000)\n"
                "    return dt.strftime('%Y%m%d-%H:%M:%S') + f':{value % 1000:03d}'"
            ),
        }

    def relaxed_spec(self) -> semzip_pure.CandidateSpec:
        spec = semzip_pure.CandidateSpec(
            tag="TS",
            pattern=r"^(\d{8})-(\d{2}:\d{2}:\d{2}):(\d{3})",
            replacement="{placeholder}",
            program=self.health_timestamp_program(),
            store_group=0,
            kind="open_function",
            semantic_class="class_1_composed_numeric",
        )
        return semzip_pure.width_agnostic_numeric_replay_spec(spec)

    def test_replay_spec_generalizes_digits_and_marks_shared_layout(self) -> None:
        relaxed = self.relaxed_spec()

        self.assertEqual(r"^(\d+)-(\d+:\d+:\d+):(\d+)", relaxed.pattern)
        self.assertEqual(r"^(\d+)-(\d+:\d+:\d+):(\d+)$", relaxed.program["group_regex"])
        self.assertTrue(relaxed.program["_layout_polymorphic_shared_delta"])

    def test_variable_widths_round_trip_through_one_delta_stream(self) -> None:
        relaxed = self.relaxed_spec()
        values = [
            "20171223-22:15:35:011",
            "20171223-22:15:35:11",
            "20171224-0:10:10:137",
            "201811-14:9:26:912",
            "201811-0:0:0:190",
        ]
        spec = dataset_extract.ExtractSpec(
            tag=relaxed.tag,
            pattern=relaxed.pattern,
            kind=relaxed.kind,
            store_group=relaxed.store_group,
            replacement=relaxed.replacement,
            context_tag=json.dumps(relaxed.program, sort_keys=True),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            metadata = dataset_extract.write_tag_stream(output_dir, spec, values)
            decoded = dataset_extract.read_tag_stream(output_dir, metadata)
            files = semzip_pure.compact_stream_files(metadata)

        self.assertEqual("layout_shared_delta", metadata["open_numeric_codec"])
        self.assertEqual(3, len(files))
        self.assertEqual(values, decoded)

    def test_literal_replacement_affixes_do_not_duplicate(self) -> None:
        spec = semzip_pure.CandidateSpec(
            tag="TS",
            pattern=r"\[([A-Z][a-z]{2}) ([A-Z][a-z]{2}) (\d{2}) (\d{2}:\d{2}:\d{2}) (\d{4})\]",
            replacement="[{placeholder}]",
            program={
                "op": "python_exec",
                "group_regex": r"^(?:\[([A-Z][a-z]{2}) ([A-Z][a-z]{2}) (\d{2}) (\d{2}:\d{2}:\d{2}) (\d{4})\])$",
                "code": (
                    "def forward(groups):\n"
                    "    dt = datetime.strptime(' '.join(groups), '%a %b %d %H:%M:%S %Y')\n"
                    "    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\n\n"
                    "def inverse(record):\n"
                    "    dt = datetime.fromtimestamp(int(record['stored'][0]) / 1000000)\n"
                    "    return dt.strftime('%a %b %d %H:%M:%S %Y')"
                ),
            },
            store_group=0,
            kind="open_function",
            semantic_class="class_1_composed_numeric",
        )
        relaxed = semzip_pure.width_agnostic_numeric_replay_spec(spec)
        value = "[Thu Jun 09 06:07:04 2005]"
        match = re.fullmatch(relaxed.pattern, value)

        self.assertIsNotNone(match)
        rendered = semzip_pure.validation_render_value(relaxed, match.group(0))
        rebuilt = semzip_pure.format_replacement_fast(relaxed.replacement, rendered, match)
        self.assertEqual(value, rebuilt)

    def test_oversized_residual_integer_uses_exact_string_stream(self) -> None:
        values = [
            "123456789012345678901234",
            "000123456789012345678901",
            "987654321098765432109876",
        ]
        spec = dataset_extract.ExtractSpec(
            tag="BIG",
            pattern=r"\b\d+\b",
            kind="int_delta_fixed_width",
            store_group=0,
            replacement="<BIG>",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            metadata = dataset_extract.write_tag_stream(output_dir, spec, values)
            decoded = dataset_extract.read_tag_stream(output_dir, metadata)

        self.assertEqual("string_mtf_rank", metadata["kind"])
        self.assertEqual(values, decoded)

    def test_regex_anchor_guard_distinguishes_literals_from_regex_syntax(self) -> None:
        rejected = [
            r"(\d+)",
            r"^([A-Za-z0-9._-]+)$",
            r"^([A-Z][a-z]{2})$",
            r"(?<![\d.])(\d+)(?![\d.])",
        ]
        accepted = [
            r"port( +)(\d+)",
            r"(?:\d{1,3}\.){3}\d{1,3}",
            r"(\d+):(\d+)",
            r"\[([A-Z][a-z]{2}) (\d+)\]",
        ]

        for pattern in rejected:
            self.assertFalse(
                semzip_pure.regex_has_fixed_alpha_or_structure(pattern),
                pattern,
            )
        for pattern in accepted:
            self.assertTrue(
                semzip_pure.regex_has_fixed_alpha_or_structure(pattern),
                pattern,
            )


if __name__ == "__main__":
    unittest.main()
