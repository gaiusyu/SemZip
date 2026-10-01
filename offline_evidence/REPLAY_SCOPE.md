# Response replay boundary

The shipped `source/trainer/semzip_pure.py` provides a cache-only proposal mode, not a complete recorded-HTTP-response replay interface:

- Lines 2919–2926 read an existing parsed proposal cache and attach an `exact_cache` marker.
- Lines 2992–2998 return an empty-functions payload on a cache-only miss.
- Lines 11121 and 11175 gate refresh behavior on whether cache-only mode is active.

Consequently, replacing a fresh request with this mode can change control flow and is not by itself proof that replaying the 202 original responses reproduces all 16 exact program hashes. The R62 fitting/recovery helpers examined here replay saved programs; they do not supply an end-to-end recorded-response trainer entry point. The bounded response-hook controls described below provide a separate, explicitly scoped validation.

The separate R57/R58 response hook was subsequently reused in the bounded controls under `validation_controls/fresh_response_boundary/`. The uniform UTC0/C run retains 14 identical plan files and two differences. Explicit diagnostics in one fixed non-UTC zone (its name is withheld in this anonymous copy) reproduce Linux and Mac, while the original training timezone remains unpinned. These are response-content-boundary controls, not HTTP/provider replay or a uniform sixteen-file UTC0 success. The original saved sample and program/policy files are unchanged. A package-relative `tools/run_suite.py` entry under that control subtree has also reproduced all eighteen recorded outcomes using only the included source, samples and exchanges.
