from __future__ import annotations

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import run_main_batch
import semzip_pure


class FakeResponse:
    def __init__(self, raw: str) -> None:
        self._raw = raw.encode("utf-8")
        self.status = 200
        self.headers = {
            "Content-Type": "application/json",
            "X-Request-ID": "request-123",
        }

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def getcode(self) -> int:
        return self.status

    def read(self) -> bytes:
        return self._raw


class ApiProvenanceTest(unittest.TestCase):
    def test_records_envelope_without_changing_function_cache(self) -> None:
        provider_payload = {
            "id": "chatcmpl-test",
            "model": "gpt-4o-2024-08-06",
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {"functions": [{"tag": "IP", "regex": "x"}]}
                        )
                    }
                }
            ],
            "usage": {
                "prompt_tokens": 101,
                "completion_tokens": 23,
                "total_tokens": 124,
            },
            "cost_usd": 0.00125,
        }
        raw_response = json.dumps(provider_payload, separators=(",", ":"))
        args = argparse.Namespace(
            force_llm=False,
            llm_family_cache_fallback=False,
            llm_global_cache_fallback=False,
            llm_cache_only=False,
            model="gpt-4o",
            temperature=0.0,
            api_base="https://example.invalid/v1/chat/completions",
            timeout=180,
            api_retries=2,
        )
        secret = "never-write-this-key"

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_root = Path(temp_dir)
            cache_path = cache_root / "family_0001_test.json"
            with mock.patch.dict(os.environ, {"PARE_LLM_API_KEY": secret}), mock.patch.object(
                semzip_pure.urllib.request,
                "urlopen",
                return_value=FakeResponse(raw_response),
            ):
                result = semzip_pure.call_llm("example prompt", args, cache_path)

            expected_cache = {
                "functions": [{"tag": "IP", "regex": "x"}],
                "_proposal_cache_mode": "api",
            }
            self.assertEqual(expected_cache, result)
            self.assertEqual(expected_cache, json.loads(cache_path.read_text(encoding="utf-8")))

            with mock.patch.object(
                semzip_pure.urllib.request,
                "urlopen",
                side_effect=AssertionError("cache hit must not call the API"),
            ) as cached_urlopen:
                cached_result = semzip_pure.call_llm("example prompt", args, cache_path)
            self.assertEqual("api", cached_result["_proposal_cache_mode"])
            cached_urlopen.assert_not_called()

            request_paths = list(cache_root.rglob("*.request.json"))
            response_paths = list(cache_root.rglob("*.response.json"))
            raw_paths = list(cache_root.rglob("*.response.raw.txt"))
            http_paths = list(cache_root.rglob("*.http.json"))
            self.assertEqual(1, len(request_paths))
            self.assertEqual(1, len(response_paths))
            self.assertEqual(1, len(raw_paths))
            self.assertEqual(1, len(http_paths))
            self.assertEqual(provider_payload, json.loads(response_paths[0].read_text(encoding="utf-8")))
            self.assertEqual(raw_response, raw_paths[0].read_text(encoding="utf-8"))

            request_record = json.loads(request_paths[0].read_text(encoding="utf-8"))
            self.assertEqual("<redacted>", request_record["headers"]["Authorization"])
            for path in cache_root.rglob("*"):
                if path.is_file():
                    self.assertNotIn(secret, path.read_text(encoding="utf-8"))

            usage = run_main_batch.cache_usage(cache_root)
            self.assertEqual(1, usage["response_files"])
            self.assertEqual(1, usage["response_files_with_usage"])
            self.assertEqual(101, usage["input_tokens"])
            self.assertEqual(23, usage["output_tokens"])
            self.assertEqual(124, usage["total_tokens"])
            self.assertEqual("gpt-4o-2024-08-06", usage["actual_model_ids"])
            self.assertAlmostEqual(0.00125, usage["provider_cost_usd"])
            run_main_batch.validate_api_provenance("Test", usage, 1)

    def test_rejects_missing_usage(self) -> None:
        usage = {
            "response_files": 1,
            "response_files_with_usage": 0,
            "actual_model_ids": "gpt-4o-2024-08-06",
        }
        with self.assertRaisesRegex(RuntimeError, "usage exists in 0 of 1"):
            run_main_batch.validate_api_provenance("Test", usage, 1)


if __name__ == "__main__":
    unittest.main()
