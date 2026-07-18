from __future__ import annotations

import re
import unittest

import pare_dataset_extract as dataset_extract
import semzip_pure


class TimestampWidthGeneralizationTest(unittest.TestCase):
    def test_syslog_width_variants_round_trip(self) -> None:
        narrow = semzip_pure.CandidateSpec(
            tag="TS",
            pattern=r"^([A-Z][a-z]{2}( +)\d{2}( +)\d{2}:\d{2}:\d{2})",
            replacement="{placeholder}",
            program={
                "op": "python_exec",
                "group_regex": r"^(([A-Z][a-z]{2}( +)\d{2}( +)\d{2}:\d{2}:\d{2}))$",
                "code": (
                    "def forward(groups):\n"
                    "    text = groups[0]\n"
                    "    dt = datetime.strptime(text, '%b %d %H:%M:%S')\n"
                    "    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\n\n"
                    "def inverse(record):\n"
                    "    micros = int(record['stored'][0])\n"
                    "    dt = datetime.fromtimestamp(micros / 1000000)\n"
                    "    return dt.strftime('%b %d %H:%M:%S')"
                ),
            },
            store_group=0,
            kind="open_function",
            semantic_key="raw_time:syslog:test",
            semantic_class="class_1_composed_numeric",
            value_type="numeric",
        )
        generalized = semzip_pure.global_replay_spec(narrow)

        self.assertTrue(generalized.program.get("_timestamp_width_generalized"))
        self.assertNotIn(r"\d{2}", generalized.pattern)
        for value in ("Dec 10 06:55:46", "Jan  1 00:12:28", "Jan 1 0:2:3"):
            match = re.match(generalized.pattern, value)
            self.assertIsNotNone(match)
            raw = match.group(generalized.store_group)
            self.assertEqual(value, dataset_extract.render_open_function_exact(raw, generalized.program))


if __name__ == "__main__":
    unittest.main()
