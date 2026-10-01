# R76 track E: decode-time safety boundary for generated `python_exec` code

Work dir: `<WORKDIR>/r76_additional_20260929/safety/`
Design: `../DEV_DESIGN_R76_zh.md` section E. Everything outside this directory was read only. No program,
archive, plan or runtime file was modified; nothing was re-encoded; no LLM/API call was made.

Provenance: the V1 files (validator, static check, archive scan, enforcing decoder, tests, negative
controls NC1-NC3, smoke run, full run `full_20260929/`) were produced 02:46-02:56 UTC on 2026-09-29 by an
earlier run of this track. A later run re-audited that work, found two escapes that V1 accepts (section 2.3),
and added `v2/` (V2 validator, rerun of every check, NC4, second full run). V1 outputs were left untouched.

## 0. Result in one paragraph

Generated code is executed **in-process** by `exec` with a restricted `__builtins__` and a runtime AST node
allow-list, but with **no sandbox, no subprocess isolation, no CPU/memory/time limit**, as the same uid, and
with an unfiltered `tarfile.extractall` of the archive. The static allow-list validator (separate module,
never compiles or runs the code) finds **no violation in any LLM-written code** among 89 unique programs
(86 `python_exec`, 3 context projectors) in all published and gated plans. The only strict rejections (12
unique codes, 144 violations) are all the six underscore identifiers of the runtime's own fixed, non-LLM
space/digit-layout wrapper (`semzip_pure.space_layout_python_exec_program`). They make the pre-registered strict
decoder fail closed on 5 of 16 pool archives (Zookeeper, Mac, Hadoop, OpenSSH, BGL). With the post-hoc `template`
policy, the pinned wrapper is exempt and its LLM prefix still gets every strict rule. Under that policy
all 16 archives decode from the archive alone with per-block and full-file SHA-256 equal to the originals.
The re-audit found that V1 (and the frozen runtime) let a **context projector reach `sys` via `re.enum.sys`**
and did not parse nested format-spec fields. V2 closes both. No real program changes verdict. NC4 shows
the frozen and V1 decoders executing such code from a tampered archive, while V2 fails closed.
A later coverage pass (`coverage_20261002/`, section 3.1) applies the unchanged V2 check to the syntheses this scope
did not include (the 16 repeated greedy syntheses, the complete reruns, the unseen-source runs) and to the
hand-written rule library. It adds 45 distinct LLM-written programs (**134 in total**) and again finds no violation
outside the runtime's fixed wrapper (0 in LLM-written code); the 11 hand-written library programs pass both policies.

## 1. Audit: how `python_exec` code is executed (file:line)

The runtime is identical in R69, R71 and R73. These sha256 values are the same in
`r69_fresh_main_20260923/source`, `r71_strict_main_20260923/source` and `r73_quality_gate_20260927/r73_qg/art/source`:

- `runtime/pare_dataset_extract.py`: 628de835...
- `runtime/semzip_pure.py`: ea6bf678...
- `program_extension.py`: 177550cf...
- `execution_environment.py`: cb3f556f...
- `backend/semantic_codec_frontend.py`: f1ffd8fc...
- `backend/run_bounded_semantic.py`: 033d4d3c...

The guards are identical too: `guarded_backend.py` 80b91d00... and `guarded_backend_v2.py` 72bca1ca....
`trainer/pare_dataset_extract.py` differs from the runtime copy only after line 4856, so the code-execution section
(4365-4700) is byte-identical in both. Paths below are relative to `r73_qg/art/source/`.

### 1.1 The only two `exec` sites reachable in R69/R71/R73

| what | where |
|---|---|
| node allow-list `SAFE_PYTHON_EXEC_NODES` (Module, FunctionDef, arg(s), Return, Assign, Expr, If, For, **While**, Break, Continue, Pass, Name, Constant, List, Tuple, Dict, Subscript, Slice, Index, BinOp incl. **Pow/LShift/RShift**, UnaryOp, BoolOp, Compare, IfExp, JoinedStr, FormattedValue, Call, Attribute) | `runtime/pare_dataset_extract.py:4412-4464` |
| lazy-import shim: `__import__` only for `_strptime`, `time` (needed by `datetime.strptime`) | `:4467-4472` |
| namespace `SAFE_PYTHON_EXEC_GLOBALS` = datetime (class), timedelta, int, str, len, float, round, abs, min, max, `__import__`=shim | `:4475-4487` |
| name block-list `BLOCKED_PYTHON_EXEC_NAMES` = open, eval, exec, compile, globals, locals, vars, `__import__`, input | `:4488` |
| per-process compile caches keyed by code string | `:4489-4490` |
| AugAssign rewritten to Assign before checking | `:4493-4502` |
| `compile_generated_python_exec`: `ast.parse`, then node check (names starting `__` or block-listed, attributes starting `__`, calls to block-listed names), then `env={"__builtins__": SAFE_PYTHON_EXEC_GLOBALS, **SAFE_PYTHON_EXEC_GLOBALS}`, then **`exec(compile(tree,"<llm_python_exec>","exec"), env, env)`** (runs the module top level), then fetches `forward`/`inverse` | `:4505-4531` (exec at `:4524`) |
| `compile_generated_context_projector`: same checks; `re.<attr>(...)` calls whose callee is literally `re.X` must be search/match/fullmatch with a literal, `is_open_llm_regex_safe` pattern; namespace = the same plus **`re` (module)**; **`exec` at `:4572`** | `:4534-4577` |
| projector output type/size check (str/int/None, <=1024 B) | `:4580-4597` |
| forward/inverse wrappers (dict with list `stored`/`layout`, one stored scalar, layout bytes 0..255, inverse must return str) | `:4600-4655` |
| `is_open_function_program_safe` (size caps 2/8/12 KiB) **compiles, i.e. executes the top level of, `code` and `context_code`** | `:4378-4409` (compile calls at `:4401`, `:4406`) |
| every archive program goes through `open_function_program` -> the predicate above ("Unsafe open function program") | `:4658-4666` |
| `int_expr_delta` uses its own AST interpreter `safe_eval_int_expr` (no exec) | `:4669-4723` |
| `llm_multi_program` streams (`program_extension.install(c)`, installed at `pare_dataset_extract.py:11325-11330`) call `c.compile_generated_python_exec` through the module attribute | `program_extension.py:37` (decode `read`), `:100` (encode `write`), `:69-95` (encode-side forward/inverse round-trip, errors mean exception route) |

Not reachable in R69/R71/R73: `backend/full_plan_frontend.py:139-146` also has `exec` (SAFE_BUILTINS + math + re, no
AST check), but only `backend/run_full_plan_experiment.py:9` imports it.

### 1.2 Decode time (archive-only)

1. `run_bounded_semantic.decode` (`backend/run_bounded_semantic.py:172-230`) checks the manifest and file set, then
   starts a `ProcessPoolExecutor` (`:198`, default **fork** start method on Linux; the R76 runs record `mp_start_method=fork`).
   `expected` hashes are used only for verification (`:180`, `:214-218`).
2. Per block, `_decode_job` (`:71-83`) runs the native C++ `decompress` through `subprocess.run` with **no timeout**
   (`_native`, `:24-31`), then calls `semantic_codec_frontend._decode_block` (`backend/semantic_codec_frontend.py:333-352`).
3. `extract_semantic_archive` (`semantic_codec_frontend.py:131-135`) does **`tarfile.extractall` with no member
   filtering**. Python 3.9.2 has no extraction filter, so `..`, absolute paths and links in a crafted archive are honoured.
4. `metadata.json` is read. `activate_execution_environment` (`execution_environment.py:9-18`) requires exactly
   `{"version":1,"timezone":"UTC0","lc_time":"C"}` and sets the process-global TZ and LC_TIME.
5. `restore_text` (`pare_dataset_extract.py:10639`) decodes each stream. Programs come from the archive metadata
   (`open_function_program`), are checked by the runtime AST guard, `exec`'d at load and called once per value.
   Context code stored in a program's `context_code` is compiled, which executes its top level, inside the safety
   predicate (`:4406`), even when the stream never calls it. For `open_context_delta`, the decoder program has
   `context_code` removed at encode (`:9271-9290`).
6. Frozen guard wrappers: `guarded_backend_v2._decode_job` (`r73_qg/art/frozen/guarded_backend_v2.py:96-107`)
   clears data caches and compiled-callable caches at block entry and exit (`runtime_cache_boundary.py`).
   There is no cross-block state, but also no isolation.

### 1.3 Encode time (online) and training/gate (offline)

- Online encode `guarded_backend_v2.encode` (`frozen/guarded_backend_v2.py:110-118`) calls `guarded_backend.encode`
  (`frozen/guarded_backend.py:199-216`) and then `run_bounded_semantic.encode`. Worker processes run `forward` on every
  matched value. `_attempt` (`guarded_backend_v2.py:31-71`) decodes the real written semantic archive with cold caches
  and compares bytes. Recoverable semantic errors fall back to the empty plan (`guarded_backend.py:144-172`).
  Same namespace and same `exec` sites as decode. R73 `formal_full.py:25` runs encode in-process (4 workers), then a
  separate archive-only decode through the frozen CLI subprocess (`formal_full.py:31`).
- Training: `train_k.py` -> `trainer/run_blocks_pure.py` (`ProcessPoolExecutor`, `:480`, `:498`) -> `trainer/semzip_pure.py`.
  The LLM call is `urllib`, `timeout=240` (`:3052`, `:13320`). Candidates are validated or executed through
  `dataset_extract.is_open_function_program_safe` (`trainer/semzip_pure.py:4319, 4586, 4653, 4807, 5651, 5667, 5729,
  5875, 6373`), which has the same exec sites. The gate (`qg_select.py:90-120`) goes through `lib.train_cost` (`lib.py:88-97`),
  which runs `fit_storage_v2` and then a guarded encode in a `python -c` subprocess (`lib.py:_encode_child`).
  It uses a clean env with PARE_/YUNWU_/SEMZIP_/*_API_KEY removed, but has no resource limits.
- The runtime rewrites LLM programs deterministically at encode: `semzip_pure.py:924-929` (`rename_python_exec_entrypoints`:
  `def forward(` becomes `def _orig_forward(`, `def inverse(` becomes `def _orig_inverse(`), and `:932-1067`
  (`space_layout_python_exec_program`) appends a fixed wrapper that defines `_space_lengths`, `_digit_widths`,
  `_apply_space_lengths`, `_apply_digit_widths`, `forward`, `inverse`. Call sites are `:1079` and `:4513`.
  The wrapped string is what gets published and stored in archives.

### 1.4 Boundary summary

| property | frozen runtime (R69/R71/R73) |
|---|---|
| namespace / builtins | only the 10 names above (+`re` for context); `__builtins__` replaced, so no `open`, `type`, `getattr`, `list`, ... |
| imports | Import nodes not allowed; `__import__` shim allows `_strptime`/`time` only |
| dunder access | Name/Attribute starting `__` rejected. **Single `_` allowed. Not checked: str.format fields (`'{0.__class__}'.format(x)`) and non-underscore module attributes (`re.enum.sys`)** |
| top-level code | executed at load, including inside the safety predicate |
| loops / arithmetic | `while` and `**`/`<<` allowed. **No CPU, memory or time limit anywhere** (no `resource`, `setrlimit`, `alarm` or decode timeout in runtime, backend or guards) |
| isolation | **none**: same process as the decoder worker (fork), same uid (root in this container), full filesystem and network access |
| archive extraction | **unfiltered `tarfile.extractall`** |
| determinism | TZ/LC_TIME pinned by the archive-declared environment; `datetime.now` is not blocked |

## 2. Static allow-list validator

### 2.1 V1: `pyexec_validator.py` (VERSION `R76-PYEXEC-ALLOWLIST-V1-20260929`)

Pure `ast.parse`; the code is never compiled or run. `check(code, kind)` returns every violation with line and column.
`enforce()` raises `UnsafeGeneratedProgram`.

Hard rules:

- `E_IMPORT`: Import/ImportFrom.
- `E_UNDERSCORE`: a name, def, argument, keyword or attribute starting with `_`.
- `E_FORBIDDEN_NAME`: exec, eval, compile, open, `__import__`, globals, locals, vars, getattr, setattr, delattr, input,
  breakpoint, memoryview, whether called or merely referenced.
- `E_SCOPE`: global/nonlocal.
- `E_CLASS`: class definitions.
- `E_LAMBDA`.
- `E_GENERATOR`: genexp, yield, async.
- `E_COMPREHENSION`.
- `E_EXCEPT`: try/raise.
- `E_WITH`.
- `E_OTHER_NODE`: node-type allow-list.
- `E_FREE_NAME`: builtin allow-list = the runtime namespace minus `__import__`: datetime, timedelta, int, str, len,
  float, round, abs, min, max, plus `re` for context code only. The programs actually use datetime, int, str, len,
  float, round and `re`.
- `E_INTROSPECTION`: gi_frame, f_globals, tb_frame, mro, ...
- `E_FORMAT`: format_map; `.format` on a non-literal; literal fields containing `.` or `[`.
- `E_DECORATOR`.
- `E_STAR`.
- `E_ENTRYPOINT`.

Report-only warnings:

- `W_WHILE_TRUE_NO_BREAK`: `while <truthy const>` without its own break.
- `W_WHILE`.
- `W_TOPLEVEL_STMT`.
- `W_RECURSION`.
- `W_NONDETERMINISTIC`.

### 2.2 Post-hoc secondary policy `template` (same file)

Added only after the strict static check showed that every rejection came from the runtime's own wrapper
(section 1.3). A code string is accepted iff all of the following hold:

- (a) it ends with the exact frozen wrapper literal. That literal is read by AST from `runtime/semzip_pure.py`,
  and its sha256 is pinned to f1389aec...
- (b) the prefix has exactly one `def _orig_forward(` and one `def _orig_inverse(`.
- (c) the prefix, with those two names mapped back, passes **every** strict rule.

Nothing else is relaxed, and code without the wrapper is checked strictly. The strict results are always
reported first.

### 2.3 V2: `v2/pyexec_validator.py` (`R76-PYEXEC-ALLOWLIST-V2-20260929`)

A re-audit with `reach_probe.py` (result in `reach_probe_result.json`) walked the attribute graph over non-underscore
names from the exact runtime namespace (Python 3.9.2, depth 4, including instances of every allowed type).

- `python_exec` namespace: **0** module objects or dangerous builtins reachable.
- context namespace: through `re` the walk reaches copyreg, enum, functools, sre_compile and sre_parse, and
  **`re.enum.sys` is `sys`**. `re.enum.sys.modules['os']` is therefore reachable with no underscore. Neither the
  runtime (its `re` check only inspects callees that are literally `re.X`) nor V1 blocks this.
- V1 also did not parse replacement fields nested in a format spec (`'{0:{1.__class__}}'.format(a, b)`).

V2 adds three rules and relaxes nothing:

- `E_RE_USE` (context code): `re` may appear only as `re.search/match/fullmatch(<literal str>, ...)`, and may not be
  rebound by assignment, argument or def.
- `E_FORBIDDEN_ATTR`: an attribute named like a forbidden builtin, or one of copyreg, enum, functools, sre_compile,
  sre_parse, sys, modules.
- `E_FORMAT` now also inspects nested format-spec fields.

`v2/enforced_decode.py`, `v2/run_verify.py`, `v2/static_check.py`, `v2/scan_archives.py` are copies whose only
change is their docstring (static_check/scan_archives are byte-identical). They import the V2 validator next to them.

## 3. Static check of all published and gated programs

Commands: `python3 static_check.py static_check_result.json` (V1, root dir) and the same in `v2/`.
Both give identical verdicts on the real programs.

| source group | files | files with code | code occurrences | unique codes | strict: files rejected / unique rejected | template: rejected |
|---|---:|---:|---:|---:|---|---|
| R73 pool publications (`r73_qg/runs/publish/pool/*/program.json`) | 16 | 16 | 100 | 27 | 5 / 3 (BGL, Hadoop, Mac, OpenSSH, Zookeeper) | 0 |
| R73 pool storage policies | 16 | 0 | 0 | 0 | 0 | 0 |
| R71 / SemZip-1 publications (`training/*/publication.json` -> deployed_plan) | 16 | 15 | 112 | 27 | 6 / 4 (Hadoop, Mac, OpenSSH, OpenStack, Spark, Zookeeper) | 0 |
| R71 storage policies | 16 | 0 | 0 | 0 | 0 | 0 |
| R73 art/deployments (copy of R71) | 16 | 15 | 112 | 27 | 6 / 4 | 0 |
| R73 per-candidate gated plans (`runs/qg/c0..c4/*/selected_plan.json`) | 80 | 77 | 358 | 80 | 34 / 11 | 0 |
| R73 qg1c0 publications | 14 | 14 | 86 | 25 | 6 / 4 | 0 |
| R75 evolution update programs (Thunderbird b013; Spark b021,b034,...,b084) | 8 | 2 | 11 | 8 | 1 / 1 (Spark b021) | 0 |
| R75 evolution storage | 8 | 0 | 0 | 0 | 0 | 0 |
| R75 evolution package copies | 8 | 2 | 11 | 8 | 1 / 1 | 0 |
| R75 evolution gated candidate plans | 40 | 9 | 36 | 12 | 6 / 2 | 0 |
| R75 empty program (+cfb_work) | 18 | 0 | 0 | 0 | 0 | 0 |
| R75 empty storage | 18 | 0 | 0 | 0 | 0 | 0 |
| **all (unique by code sha)** | 274 | | | **89** (86 python_exec, 3 context) | **12 unique rejected, 144 violations, all `E_UNDERSCORE`** | **0** |

Other counts:

- Warnings: `W_WHILE` 96, all inside the fixed wrapper (8 while loops x 12 codes).
- LLM-written code: 0 warnings. None of `W_WHILE_TRUE_NO_BREAK`, `W_TOPLEVEL_STMT`, `W_RECURSION` or
  `W_NONDETERMINISTIC` occurs in any program.
- Union of attributes used by the 89 codes: append, day, fromtimestamp, get, group, hour, isdigit, join, minute, month, search,
  second, split, strftime, strip, strptime, timestamp, zfill.

**Every violation, verbatim.** All 12 rejected codes have this identical list of 12 violations
(`rule`, `detail`, offending source line):

```
E_UNDERSCORE  definition name '_orig_forward'         def _orig_forward(groups):
E_UNDERSCORE  definition name '_orig_inverse'         def _orig_inverse(record):
E_UNDERSCORE  definition name '_space_lengths'        def _space_lengths(text):
E_UNDERSCORE  definition name '_digit_widths'         def _digit_widths(text):
E_UNDERSCORE  definition name '_apply_space_lengths'  def _apply_space_lengths(text, lengths):
E_UNDERSCORE  definition name '_apply_digit_widths'   def _apply_digit_widths(text, widths):
E_UNDERSCORE  name '_orig_forward'                    base = _orig_forward([raw])
E_UNDERSCORE  name '_space_lengths'                   spaces = _space_lengths(raw)
E_UNDERSCORE  name '_digit_widths'                    widths = _digit_widths(raw)
E_UNDERSCORE  name '_orig_inverse'                    text = _orig_inverse({'stored': record.get('stored', []), 'layout': base_layout})
E_UNDERSCORE  name '_apply_space_lengths'             text = _apply_space_lengths(text, spaces)
E_UNDERSCORE  name '_apply_digit_widths'              text = _apply_digit_widths(text, widths)
```

The 12 rejected codes (sha256) and where they occur:

- 7a0a0bd1e7ec6e4457254fc658b6ac51181e8e8803cd800618fd3544a6a0b44f: pool BGL; R73 c3/BGL
- e2836ed0901f65c55ed62ee7dc11224fd7d8a5cf128539469fa3d41c599f4c5c: pool Hadoop and Zookeeper; R71 Hadoop and Zookeeper; qg1c0; c0-c4 Hadoop and Zookeeper
- 3728f68947e15bf6db49345234016f01eaee3b3546eb32d91632b9f5105e9608: pool Mac and OpenSSH; R71 Mac and OpenSSH; qg1c0; several c*; R75 Thunderbird b013 c4
- 214c207667408dacadfef883c68d5af4e1043f6b55149dda2869fc20105c4ad7: R71 OpenStack; qg1c0; c0,c1,c3,c4 OpenStack
- b5354089f4996a9a4dfdf1919a60496f0eb1a14352664f5e400e4169bcb1ba0c: R71 Spark; qg1c0; c0,c1,c2,c4 Spark
- 1e65e9c3ee33c61ced657f9c53f66ecb650ba2d4d85d76a09c704798bcb7c7f9: R75 Spark b021 publication, package copy, c0-c4
- 73aba84bc59b... (c1/BGL), ecc8f3a51bf6... (c1/HPC), 64f11e521d82... (c1/Mac), 3e1341688b2d... (c2/Mac),
  d87a45ae10a5... (c3/Mac), bbdc8e5024a7... (c4/Android)

Full per-file and per-code records, with the source line of each violation, are in `static_check_result.json`
(and `v2/static_check_result.json`).

The code actually embedded in the 16 pool archives was also scanned (`scan_archives.py`: tar index + metadata.json read
in memory; nothing extracted or run):

- 23 unique codes: 21 python_exec and 2 context. The 2 context codes are carried in `context_tag` only and are
  never compiled at decode.
- 3 are strict-rejected, the same wrapper case as above. Strict-rejected blocks: BGL 48/48, Hadoop 4/4,
  OpenSSH 7/7, Mac 2/2, Zookeeper 1/1.
- 0 are rejected under the template policy.
- 0 tar member problems over 3,797 blocks.
- The V1 scan and the V2 scan are identical.

### 3.1 Coverage extension (`coverage_20261002/`)

A later pass checked, with the **unchanged** V2 validator and the unchanged V2 extraction and verdict code (both
imported from `v2/`; the scripts check their SHA-256 before running), every program that the scope above did not
include. Scripts: `coverage_check.py` (static check of plans and publications, both policies; stdout in
`coverage_check.log`) and `scan_new_archives.py` (the code embedded in the formal archives of the new publications,
through the unchanged `v2/scan_archives.py`: tar index and `metadata.json` read in memory, nothing extracted or run;
stdout in `scan_new_archives.log`). Nothing was re-encoded, no program or plan was modified, and no model was called.
Each violation is classified by origin: inside the runtime's fixed space/digit-layout wrapper (pinned template),
the runtime's deterministic renaming of the LLM's `forward`/`inverse` to `_orig_forward`/`_orig_inverse`
(`def` lines of the prefix), or the program body.

Scope (main): R73 `g1`, the 16 repeated greedy (T=0.0) syntheses `train_k/<D>/t0.0_k1`; R76-C (`../variance/`),
the 40 syntheses of the complete reruns (HPC, Hadoop, HDFS, Spark x r2, r3 x c0-c4), their 40 gated candidate
plans, 8 pool-selected plans, 8 publications and 8 storage policies; R76-G (`../unseen/`), 20 syntheses, 20 gated
plans, 4 pool plans, 4 publications and 4 storage policies; R76-B2 (`../library/`, hand-written, no LLM), 16 replay
plans, 16 gated plans, 16 publications and 16 storage policies, reported separately. Supplementary scope: the
code embedded in each synthesis' training-archive metadata and every gate/pool evaluation plan (3,495 files). 3,731
files were read; none was missing. The supplementary scope contains no code that is not also in the main scope.

| source (main scope) | unique codes | already among the 89 | strict: unique rejected / violations | violations by origin | template: rejected | warnings |
|---|---:|---:|---|---|---:|---|
| R73 g1 (greedy repeats) | 30 (28 python_exec, 2 context) | 16 | 5 / 60 | wrapper 50, entry-point rename 10, body 0 | 0 | `W_WHILE` 40, all in the wrapper |
| R76-C complete reruns | 33 (31, 2) | 9 | 4 / 48 | wrapper 40, rename 8, body 0 | 0 | `W_WHILE` 32, all in the wrapper |
| R76-G unseen sources | 12 (11, 1) | 5 | 0 / 0 | - | 0 | none |
| **LLM-written, union** | **64 (59, 5)** | **19** | **7 / 84, all `E_UNDERSCORE`** | **wrapper 70, rename 14, body 0** | **0** | `W_WHILE` 56, all in the wrapper |
| R76-B2 library (hand-written) | 11 (11, 0) | 0 | 0 / 0 | - | 0 | `W_WHILE` 2, in program bodies (report-only) |

The 64 LLM-written codes add 45 to the 89 of section 3, so **134 distinct LLM-written programs** have been checked,
with no violation in LLM-written code under either policy; all 145 distinct codes (134 LLM-written, 11 hand-written)
pass the post-hoc `template` policy, and the library codes pass the strict policy too. The two library warnings are
bounded loops (`while i < 12`, `while i < 4`). Every violation, verbatim with its origin and source line, is in
`violations_verbatim.txt`; per-code verdicts (with the code) and per-group summaries are in
`coverage_check_result.json` (compacted in this copy; see the repository README, "Notes on this copy").

Archive scan of the 28 new publications (`archive_scan_summary.json`, per set `archive_scan/<label>.json`):

| track | archive sets | blocks | blocks rejected (strict) | blocks rejected (template) | tar member problems |
|---|---:|---:|---:|---:|---:|
| R76-C complete reruns | 8 | 908 | 586 (Hadoop r2, r3: 4/4 each; Spark r2, r3: 289/333 each; the wrapper case) | 0 | 0 |
| R76-G unseen sources | 4 | 69 | 0 | 0 | 0 |
| R76-B2 library | 16 | 3,797 | 0 | 0 | 0 |

Every code embedded in these archives is in the corresponding publication and among the codes checked above
(`archive_codes_not_in_plan_check` is empty for every set); some publication programs never reach an archive.

## 4. Validator-enforcing decode path (`enforced_decode.py`, driver `run_verify.py`)

It imports the frozen guard `guarded_backend_v2` and the runtime unchanged. Their sha256 values are checked at start
against `r71_strict_main_20260923/PUBLICATION_BINDING.json`, and the runtime file hashes are recorded. It then
installs three fail-closed checks in the parent before the pool forks, and again in every block job:

1. **Preflight** (`extract_semantic_archive` wrapped): tar members must be regular files or dirs, with no absolute
   path and no `..`, **before** extraction. After extraction, **every** code string in metadata.json is validated,
   including code that the decoder would never compile. This happens before `restore_text`.
2. **Compile hooks**: `pare_dataset_extract.compile_generated_python_exec` and `compile_generated_context_projector`
   (the only callers of `exec`, also reached by `program_extension` through the module attribute) validate their
   argument before calling the frozen function.
3. **Exec guard**: the module-global name `exec` inside `pare_dataset_extract` is shadowed by a guard that refuses
   to run outside a validated compile hook.

Any rejection raises `UnsafeGeneratedProgram`. A block job re-raises even if the frozen code swallowed the
exception. The decode fails with no fallback and no retry. Per block, `decode_logs/r76_validator_NNNNN.json`
records the policy, validator version, every validated code sha, `exec_calls`, tar members checked and the decoded sha.

`run_verify.py all`:

- Datasets are processed ascending by raw size (largest last), each in a fresh interpreter with the API/SEMZIP env
  removed, 3 workers.
- Policy `strict` runs first. `template` runs only when strict failed closed on validator rejection, and writes to a
  separate directory.
- The audit requires all of the following:
  - decode completed;
  - full sha = R68 inventory = R71 `reference.json` = the encode summary;
  - full bytes = inventory;
  - no failed blocks;
  - block counts agree;
  - every block logged PASS with sha, bytes and records equal to the inventory, 100,000 records (except the last
    block), exactly one preflight and no rejections;
  - every archive file is byte-identical (sha) to the formal R73 record;
  - archive bytes equal the formal value.

## 5. Tests and negative controls

- `test_validator.py`: V1 58 cases PASS. `v2/test_validator.py`: 71 cases PASS. It also records
  `v1_gap = {v1_accepts_context_re_escape: true, v2: false, v1_accepts_nested_format_traversal: true, v2: false}`.
- `test_enforcement.py` (V1) and `v2/test_enforcement.py`: both PASS.
  - The exec guard refuses a stray exec.
  - The hooks reject before the frozen compile: no exec, frozen cache untouched.
  - The runtime predicate now also refuses.
  - Safe code runs exactly one exec.
  - V2 additionally refuses the `re.enum.sys` context projector through the hook and through
    `is_open_function_program_safe`.
- Negative controls use tampered copies of the 1-block R73 Apache archive, kept in this directory only
  (`negative_controls/` for V1, `v2/negative_controls/` for V2):

| case | frozen decoder | V1-enforced | V2-enforced |
|---|---|---|---|
| NC1 `import os` prepended to an embedded program | rejects ("Unsafe open function program") | FAIL closed, E_IMPORT | FAIL closed, E_IMPORT |
| NC2 top-level `leak = '{0.__class__}'.format(1)` | **accepts, SHA pass** | FAIL closed, E_FORMAT | FAIL closed, E_FORMAT |
| NC3 extra tar member `../r76_nc3_escape.txt` | not run (it would write outside its temp dir) | FAIL closed, tar preflight | FAIL closed, tar preflight |
| NC4 program gets `context_code` with top level `probe = re.enum.sys.version` (harmless read of `sys`) | **accepts, SHA pass; top level executed** | **accepts, SHA pass**; `exec_calls` 3 vs 2 for the clean archive, so the context top level ran | FAIL closed, E_FORBIDDEN_ATTR 'sys' (+E_RE_USE, 'enum') |

## 6. Full decode verification of the 16 R73 pool archives (archive-only, no re-encode)

V1 run: `cd safety && AGNICE=5 ../launch.sh logs/full_verify_20260929.log python3 run_verify.py all --root full_20260929 --workers 3`

- pid 1258137, started 02:55 UTC. Log `logs/full_verify_20260929.log`, status `full_20260929/status.json`.
- Summary written at the end: `full_20260929/VERIFY_SUMMARY.json`.
- Table: `python3 summarize.py` (writes `SAFETY_SUMMARY.json`).

**V1 result, finished 03:45 UTC (elapsed 2,941 s): all 16 archives decode from the archive alone with full-file and
per-block SHA-256 equal to the originals, and every audit check passes.**

- 11/16 pass under the pre-registered strict policy.
- The other 5 fail closed under strict and pass under the post-hoc template policy.
- 3,797 blocks, 65,533,532,994 raw bytes.

| dataset | blocks | raw bytes | archive bytes | ratio | strict | template (post-hoc) | decode s | codes validated | exec calls |
|---|---:|---:|---:|---:|---|---|---:|---:|---:|
| Linux | 1 | 2349686 | 58613 | 40.09 | PASS | - | 1.1 | 2 | 2 |
| Proxifier | 1 | 2541814 | 70361 | 36.13 | PASS | - | 1.2 | 5 | 5 |
| Apache | 1 | 5135876 | 69646 | 73.74 | PASS | - | 1.3 | 2 | 2 |
| Zookeeper | 1 | 10426438 | 55045 | 189.42 | FAIL (closed) | PASS | 3.6 | 2 | 2 |
| Mac | 2 | 16879552 | 311853 | 54.13 | FAIL (closed) | PASS | 3.3 | 5 | 8 |
| HealthApp | 3 | 23529930 | 395273 | 59.53 | PASS | - | 3.2 | 1 | 3 |
| HPC | 5 | 33553503 | 732843 | 45.79 | PASS | - | 2.3 | 0 | 0 |
| Hadoop | 4 | 48595595 | 571500 | 85.03 | FAIL (closed) | PASS | 7.4 | 2 | 8 |
| OpenSSH | 7 | 73417506 | 623323 | 117.78 | FAIL (closed) | PASS | 13.7 | 4 | 21 |
| OpenStack | 3 | 61442082 | 2434257 | 25.24 | PASS | - | 5.1 | 2 | 6 |
| Android | 16 | 192270829 | 5565790 | 34.55 | PASS | - | 12.0 | 1 | 16 |
| BGL | 48 | 743185031 | 14203694 | 52.32 | FAIL (closed) | PASS | 80.3 | 1 | 48 |
| HDFS | 112 | 1577982906 | 50818812 | 31.05 | PASS | - | 62.4 | 0 | 0 |
| Spark | 333 | 2941224304 | 47170447 | 62.35 | PASS | - | 220.0 | 2 | 578 |
| Windows | 1147 | 28012696901 | 45285306 | 618.58 | PASS | - | 1165.3 | 2 | 2294 |
| Thunderbird | 2113 | 31788301041 | 490102034 | 64.86 | PASS | - | 1271.4 | 0 | 0 |

Column notes:

- "decode s" is the decoder's own timer with 3 workers on a shared, niced machine. It is not a formal timing number.
- "codes validated" is the number of distinct code strings seen by preflight and the hooks.
- "exec calls" counts `exec` calls allowed by the guard. There is at most one per distinct program per block, because the caches are
  cleared per block.
- For the FAIL (closed) rows, exec calls were 0 in every block.
- HPC, HDFS and Thunderbird carry no `python_exec` code in their archives.

The strict failures were the expected fail-closed rejections of the runtime wrapper, never a SHA mismatch. The message
was `UnsafeGeneratedProgram: R76 validator (strict) rejected python_exec code sha256=<e2836ed0 | 3728f689 | 7a0a0bd1>:
12 violation(s), first E_UNDERSCORE line 1: definition name '_orig_forward'`, raised at preflight of block 0 and of
every other block.

V2 run (chained after V1): `cd safety/v2 && AGNICE=5 ../../launch.sh logs/full_verify_v2_20260929.log ./chain_after_v1.sh`

- Launched as pid 1357486 (waited read-only on pid 1258137), then ran `python3 -u run_verify.py all --root full_v2_20260929 --workers 3`.
- Log `v2/logs/full_verify_v2_20260929.log`, status `v2/full_v2_20260929/status.json`, final `v2/full_v2_20260929/VERIFY_SUMMARY.json`.
- Started 03:45:10 UTC, **finished 04:32 UTC (elapsed 2,818 s)**. No process of this track is still running.

**V2 result: identical verdicts to V1.** strict PASS 11/16 (Linux, Proxifier, Apache, HealthApp, HPC, OpenStack, Android,
HDFS, Spark, Windows, Thunderbird); strict FAIL closed 5/16 (Zookeeper, Mac, Hadoop, OpenSSH, BGL; same wrapper
`E_UNDERSCORE` rejection at preflight of block 0, 0 exec calls); template PASS 5/5, template FAIL 0. Final SHA pass 16/16,
every audit check (per-block SHA/bytes/records, full SHA = inventory = R71 reference = encode summary, archive sha =
formal R73 record, archive bytes = formal value) passes for the final policy of every dataset. 3,797 blocks,
65,533,532,994 raw bytes. Archive bytes, ratio, codes validated and exec calls are identical to the V1 table above;
decode seconds differ only by machine load (V2: BGL 105.0, HDFS 73.7, Spark 273.1, Windows 1228.3, Thunderbird 997.5).
`python3 summarize.py` was rerun after V2 finished; `SAFETY_SUMMARY.json` now holds both finished runs.

## 7. Deviations from the design and task text

- The builtin allow-list equals the runtime namespace minus `__import__`, rather than "what the programs need". The
  programs need only datetime, int, str, len, float, round and (context) `re`, and all of them lie within it.
- Stricter than the task text: `lambda`, comprehensions, try/raise, with, decorators, star-args and generators are
  also rejected. The frozen runtime's node allow-list (`:4412-4464`) already rejects lambda, comprehensions,
  try/raise, with, generators, class, Starred and keyword arguments. It would accept decorators and `*args`/`**kwargs`
  in definitions, but no program uses them, so no program is affected. V1 in turn allows some nodes the runtime
  rejects (keyword, Set, `not`/`in`/`is`, bit operations). The effective boundary in the enforcing decoder is the
  intersection of the two checks.
- "while True without break" is report-only as specified. "Unbounded structures other than while" (design E) are
  covered structurally: no generators, recursion is report-only and bounded by Python's recursion limit.
  **Arithmetic blow-up (`10**10**9`, `'x'*10**12`) is not bounded by any static rule.**
- The `template` policy is post-hoc. It is reported separately and never replaces the strict result.
- V2 is a post-hoc fix, made after the V1 static check and the V1 full run had started. V1 artefacts are kept
  unchanged; V2 reruns every check and the full decode.
- The static-check scope also includes the R73 art/deployments copy of R71, R73 qg1c0 publications, the R75
  evolution gated candidates and storage policies.

## 8. Open problems and recommendations (for the paper's threat model)

1. **No resource limits and no process sandbox.** A hostile archive that passes both the runtime AST guard and V2 can
   still cause denial of service (CPU/memory). Cheap mitigation: per-worker `setrlimit(RLIMIT_AS, RLIMIT_CPU)` plus
   a per-block timeout, or running decode workers under seccomp/nsjail. Not implemented here, because the decode
   under test must stay the frozen one.
2. The frozen decoder is exploitable by a crafted archive: NC4 (arbitrary module access via `re.enum.sys`) and NC3
   (tar path traversal). NC2 shows information leak through format strings. Archives must be treated as code.
   The paper should state that the frozen R73 decoder trusts its archives and that the R76 enforcing decoder (V2) is
   the recommended boundary.
3. The validator is static. It depends on the exact namespace (Python 3.9.2 stdlib attribute graph). A different
   Python version needs `reach_probe.py` re-run.
4. The fixed layout wrapper uses underscore helpers. A simple product fix is to rename them (for example with a
   `semzip_` prefix) so the strict policy needs no template exemption. That would change the program bytes, so it
   was not done for the frozen archives.
5. Anonymous reproduction-package access (design G) is outside this track and is still a user decision.

## 9. Files

| file | sha256 (prefix) | role |
|---|---|---|
| pyexec_validator.py | (not listed) | V1 validator (+ template policy) |
| static_check.py | (not listed) | step 3 static check |
| scan_archives.py | (not listed) | code embedded in the 16 pool archives |
| enforced_decode.py | (not listed) | step 4 enforcing decode (import-only) |
| run_verify.py | (not listed) | step 5 driver + audit |
| test_validator.py / test_enforcement.py | 60802e7b / ddc00766 | unit tests |
| negative_controls.py | (not listed) | NC1-NC3 |
| reach_probe.py | 02025104 | attribute-graph reachability probe (found the V1 gap) |
| summarize.py | cf407a32 | results table -> SAFETY_SUMMARY.json |
| v2/pyexec_validator.py | (not listed) | V2 validator |
| v2/enforced_decode.py, v2/run_verify.py | (not listed) | V2 copies (docstring only) |
| v2/static_check.py, v2/scan_archives.py | (not listed) | byte-identical copies (import the V2 validator) |
| v2/test_validator.py, v2/test_enforcement.py, v2/negative_controls.py | bec8a8d2, ae8aacb3, (not listed) | V2 tests + NC1-NC4 |
| v2/chain_after_v1.sh | (not listed) | waits for the V1 pid, then starts the V2 full run |
| coverage_20261002/coverage_check.py | (not listed) | 3.1 static check of the programs outside the section 3 scope (unchanged V2) |
| coverage_20261002/scan_new_archives.py | (not listed) | 3.1 scan of the code embedded in the 28 new publications' archives |
| coverage_20261002/coverage_check_result.json, violations_verbatim.txt, coverage_check.log | - | 3.1 per-code verdicts, per-group summaries, every violation verbatim |
| coverage_20261002/archive_scan_summary.json, archive_scan/*.json, scan_new_archives.log | - | 3.1 archive scan records |
