# SEMZIP-REGEX-ANCHOR-GUARD-POSITION-DENUM-20260717

## Parent

`SEMZIP-LAYOUT-POSITION-DENUM-20260717`

## Controlled Change

An LLM-generated semantic function is admitted only when its regex contains
at least one observable anchor:

- a fixed alphabetic literal outside character classes; or
- fixed non-whitespace punctuation represented literally by the regex.

Regex operators, character classes, `\d`, `\w`, quantifiers, boundaries, and
lookaround syntax do not count as anchors.

Examples:

```text
(\d+)                              reject
([A-Za-z0-9._-]+)                 reject
port( +)(\d+)                     accept: fixed word "port"
(?:\d{1,3}\.){3}\d{1,3}          accept: literal dots
(\d+):(\d+)                       accept: literal colon
```

Rejected values remain in the reconstructable main text and are subsequently
eligible for the same position-first residual DEnum route as the parent.

The guard runs both when compiling fresh LLM proposals and when loading a
verified semantic replay plan. It is a general admission rule and contains no
dataset name, field name, or dataset-specific regex.

## Implementation

The implementation is `regex_has_fixed_alpha_or_structure()` in
`semzip_pure.py`. The scanner distinguishes observable literals from regex
syntax:

- Character classes such as `[A-Z]`, `\d`, `\w`, and `\S` are not anchors.
- Quantifiers, boundaries, groups, lookarounds, `.` wildcards, and `^`/`$`
  are not anchors.
- A fixed alphabetic literal outside a character class is an anchor.
- Literal non-whitespace punctuation such as `:`, `/`, `-`, or an escaped
  `\.` is an anchor.

Fresh proposals receive the compile status
`rejected_missing_regex_structure_anchor`. Frozen replay specifications pass
through the same predicate before semantic execution. Audit metadata records
the source, tag, and pattern of every rejection.

## Controlled Replay Results

Protocol: raw LogHub files, 100,000-line blocks, eight workers, the parent's
verified replay plans, zero new API calls, and full reconstructed-file SHA-256
verification.

| Dataset | Parent ratio | Guard ratio | Guard online MB/s | Archive delta | SHA |
|---|---:|---:|---:|---:|---|
| Apache | 74.2780x | 74.2780x | 0.724 | 0 B | PASS |
| OpenSSH | 118.7179x | 118.7179x | 5.097 | 0 B | PASS |
| HealthApp | 57.1420x | 57.1420x | 1.269 | 0 B | PASS |
| HPC | 17.4500x | 41.9694x | 2.652 | -1,123,356 B | PASS |

Apache, OpenSSH, and HealthApp archives are SHA-256 identical to the parent.
Across all five HPC blocks, the guard rejects exactly one replay function:

```text
tag:     TS
pattern: (\d+)
```

The function had incorrectly mixed unrelated integers into one semantic
"timestamp" stream. Once rejected, stable-position groups and DEnum route
those integers into more homogeneous streams. The HPC archive falls from
1,922,832 to 799,476 bytes, a 58.42% reduction.

## Reproduction Artifacts

```text
Remote workspace:
/root/autodl-tmp/semzip_regex_anchor_guard_position_denum_20260717

Local evidence:
evidence/{Apache,OpenSSH,HealthApp,HPC}/

Report:
../reports/regex_anchor_guard_position_denum_20260717/REPORT.md
```

Source hashes:

```text
semzip_pure.py                    f5ab8d8f11c4006f6fb454b3f676e84b22c8aa8dc0442b502fce5a6be85ba33f
pure_args_defaults.json           e80cbf98767096846e9698427f19391d70053302d0b31447da0d130574e1fd9d
run_regex_anchor_guard_replay.py  d9c304c60222213d8f0a86d3af846d904b4b7350eed721e3530c7294af0512e3
pare_dataset_extract.py           8eeb248a388a2b4a92fdc0554a88146cf429940b761b4eda1223a9b565583046
```

The five focused unit tests pass with:

```bash
PYTHONPATH=. python3 tests/test_layout_polymorphic_shared_delta.py
```

This run is a same-plan admission ablation. The fresh-proposal path contains
the same guard, but fresh API stability is not claimed by these results.
