#!/usr/bin/env python3

import unittest

from tree_sitter import Language, Parser
import tree_sitter_java

from analyze_logbench_source_census import extract_calls


class SourceCensusRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.parser = Parser(Language(tree_sitter_java.language()))

    def categories(self, statement: str) -> set[str]:
        source = f"class T {{ void run() {{ {statement}; }} }}".encode("utf-8")
        records, has_error = extract_calls(source, "repo", "T.java", self.parser)
        self.assertFalse(has_error)
        self.assertEqual(1, len(records))
        return set(records[0].categories)

    def test_printf_text_without_formatter_is_not_numeric_layout(self) -> None:
        categories = self.categories(
            'LOG.info("CQ %d %016x {}", new Object[] {t2 - t1, prev})'
        )
        self.assertNotIn("numeric_layout_format", categories)
        self.assertIn("derived_view", categories)

    def test_explicit_formatter_is_numeric_layout(self) -> None:
        categories = self.categories(
            'LOG.info(String.format("elapsed %.2f seconds", elapsed))'
        )
        self.assertIn("numeric_layout_format", categories)

    def test_remote_ip_getter_is_not_assumed_to_render_an_address(self) -> None:
        categories = self.categories('LOG.info("remote {}", command.getRemoteIp())')
        self.assertNotIn("network_address_format", categories)

    def test_host_address_call_is_network_rendering(self) -> None:
        categories = self.categories('LOG.info("remote {}", address.getHostAddress())')
        self.assertIn("network_address_format", categories)

    def test_exception_and_message_are_repeated_views(self) -> None:
        categories = self.categories('LOG.error(e.getMessage(), e)')
        self.assertIn("repeated_view", categories)


if __name__ == "__main__":
    unittest.main()
