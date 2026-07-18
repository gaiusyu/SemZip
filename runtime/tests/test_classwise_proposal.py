import unittest

import semzip_pure as semzip


class ClasswiseProposalTest(unittest.TestCase):
    def test_normalizes_three_buckets(self):
        payload = {
            "class_1_functions": [{"tag": "T", "class": "wrong", "value_type": "string"}],
            "class_2_functions": [{"tag": "N"}],
            "class_3_functions": [{"tag": "IP"}],
        }
        normalized = semzip.normalize_classwise_family_payload(payload)
        self.assertEqual([item["tag"] for item in normalized["functions"]], ["T", "N", "IP"])
        self.assertEqual(normalized["functions"][0]["class"], "class_1_composed_numeric")
        self.assertEqual(normalized["functions"][0]["value_type"], "numeric")
        self.assertEqual(normalized["functions"][2]["class"], "class_3_common_variable")
        self.assertEqual(normalized["functions"][2]["value_type"], "string")

    def test_batch_prompt_requires_every_family_and_class(self):
        families = [
            (semzip.Family(3, ("alpha",), {"alpha"}, line_indexes=[0]), ["alpha 10\n"]),
            (semzip.Family(7, ("beta",), {"beta"}, line_indexes=[1]), ["beta 20\n"]),
        ]
        prompt = semzip.build_family_batch_prompt(families, "Test", 2)
        self.assertIn("Return exactly 2 family objects", prompt)
        self.assertIn("[3, 7]", prompt)
        self.assertIn("class_1_functions", prompt)
        self.assertIn("class_2_functions", prompt)
        self.assertIn("class_3_functions", prompt)
        self.assertIn("Do not omit a family", prompt)


if __name__ == "__main__":
    unittest.main()
