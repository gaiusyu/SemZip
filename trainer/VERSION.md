# SEMZIP-CLASS3-DUAL-EVIDENCE-ABLATION-20260716

Parent: `SEMZIP-CLASS3-INTRINSIC-STRUCTURE-ABLATION-20260716`.

This controlled candidate changes only the Class 3 prompt contract. A raw
string is admissible through either evidence route:

1. Intrinsic structure: the captured value contains at least two
   non-alphanumeric separator occurrences explicitly encoded by the regex.
2. Explicit role anchor: a plain value is immediately introduced by a stable,
   unambiguous field-name literal in the regex, such as `user Alice` or
   `username=Alice`.
3. Whitespace and generic words such as `for`, `from`, `to`, and `by` do
   not qualify as role anchors. Unanchored broad captures remain forbidden.
4. An executable `user( +)(value)` example is included in both proposal paths,
   and the same dual-evidence rule is stated in verifier-repair prompts.

No program validator, extractor, codec, sampling rule, context routing rule,
residual policy, or archive format changed.
