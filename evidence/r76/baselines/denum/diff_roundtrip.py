#!/usr/bin/env python3
"""Line-aligned comparison of an original file and the official Denum decoder output.
usage: diff_roundtrip.py ORIGINAL DECODED [N_EXAMPLES]  -> JSON on stdout"""
import hashlib
import json
import sys


def main():
    orig, dec = open(sys.argv[1], "rb").read(), open(sys.argv[2], "rb").read()
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    a, b = orig.split(b"\n"), dec.split(b"\n")
    exact = sum(1 for x, y in zip(a, b) if x == y)
    ex = []
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            ex.append({"line": i, "original": x[:220].decode("latin-1"), "decoded": y[:220].decode("latin-1")})
            if len(ex) >= k:
                break
    print(json.dumps({"original_bytes": len(orig), "decoded_bytes": len(dec),
                      "original_sha256": hashlib.sha256(orig).hexdigest(),
                      "decoded_sha256": hashlib.sha256(dec).hexdigest(), "byte_exact": orig == dec,
                      "original_lf": orig.count(b"\n"), "decoded_lf": dec.count(b"\n"),
                      "original_cr": orig.count(b"\r"),
                      "lines_compared": min(len(a), len(b)), "lines_exact": exact,
                      "first_mismatches": ex}, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
