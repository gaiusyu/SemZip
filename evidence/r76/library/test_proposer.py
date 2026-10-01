#!/usr/bin/env python3
"""Unit test of LIBRARY-V1 programs against the unchanged R69 trainer validators (no LLM, no data files).
Usage: test_proposer.py R69_TRAINER_DIR"""
import random, re, sys
sys.path.insert(0, sys.argv[1])
import pare_dataset_extract as dx
import semzip_pure as sp
import proposer as P

SAMPLES = {
    "ts_syslog": ["Jun 14 15:16:01 combo sshd", "Jun  9 06:06:20 host", "Dec 31 23:59:59 x", "Feb 29 00:00:00 leap", "Jan 01 00:00:00 x"],
    "ts_ymd_hms": ["2015-10-18 18:01:47,978 INFO", "2017-05-16 00:00:00.123 x", "2016-09-28 04:30:30 y", "2000-02-29  12:00:00 z"],
    "ts_iso8601": ["at 2015-07-29T17:41:44.747Z ok", "2016-01-01T00:00:00 x", "t=2005-06-03T15:42:50.123456 y", "2019-12-31T23:59:59.5"],
    "ts_yy_slash": ["17/06/09 20:10:40 INFO", "99/12/31 23:59:59 x", "00/02/29  01:02:03 y"],
    "ts_compact": ["20171223 223756 INFO", "19991231 235959 x"],
    "ts_hms": ["took 10:30:00 ok", "at 23:59:59.999 x", "t 00:00:01.5 y"],
    "ts_epoch10": ["time 1117838570 ok", "t=1600000000 x"],
    "ipv4_port": ["from 10.10.34.11:3888 to", "/10.251.73.220:50010 x", "1.2.3.4:0 y"],
    "size_unit": ["size 1.20 KB x", "10MB y", "5 B z", "0.50 GB w", "007 MB v"],
    "percentage": ["cpu 45% x", "load 12.50% y", "0.1% z"],
    "hex_prefixed": ["addr 0x7fff0a ok", "0X1F x", "0x0000beef y", "0xDEADBEEF z"],
    "hex_bare": ["id a1b2c3d4 ok", "val 00ff x", "DEAD1234 y"],
    "fixed_point": ["t 3.14 x", "v -0.50 y", "x 007.250 z", "-0.0 k"],
    "decimal_int": ["pid 1234 x", "n -5 y", "code 007 z", "-0 w", "0 v", "123456789012345678 u"],
    "ipv4": ["from 10.0.0.1 port", "192.168.001.010 x"],
}

def check_type(t):
    name, cls, meaning, regex, repl, code, *_ = t
    program = {"op": "python_exec", "code": code}
    assert dx.is_open_llm_regex_safe(regex), ("regex unsafe", name, regex)
    assert len(regex) <= 220, ("regex too long", name, len(regex))
    assert dx.is_open_llm_replacement_safe(repl), name
    assert dx.is_open_function_program_safe(program), ("program unsafe", name)
    gprog = sp.python_exec_group_regex_variant(regex, program)
    assert gprog is not None and dx.is_open_function_program_safe(gprog), name
    rx = re.compile(regex, re.MULTILINE)
    n = 0
    for line in SAMPLES[name]:
        ms = list(rx.finditer(line))
        assert ms, ("no match", name, line)
        for m in ms:
            # compiler contract: group_regex variant, store group 0, replacement renders exact match
            rendered = dx.render_open_function_exact(m.group(0), gprog)
            assert dx._safe_replacement_format(repl, rendered, m) == m.group(0), (name, line, rendered)
            n += 1
        ok = sp.python_exec_is_semantic_three_class(ms, gprog, 0, cls, "string" if cls == P.C3 else "numeric", replacement=repl)
        assert ok, ("three-class contract", name, line)
    return n

def fuzz_timestamps():
    rnd = random.Random(0)
    t_ymd = [t for t in P.TYPES if t[0] == "ts_ymd_hms"][0]
    prog = sp.python_exec_group_regex_variant(t_ymd[3], {"op": "python_exec", "code": t_ymd[5]})
    rx = re.compile(t_ymd[3])
    ok = rej = 0
    for _ in range(20000):
        y, mo, d = rnd.randint(1970, 2099), rnd.randint(0, 13), rnd.randint(0, 32)
        h, mi, s = rnd.randint(0, 24), rnd.randint(0, 60), rnd.randint(0, 60)
        frac = rnd.choice(["", ",%03d" % rnd.randint(0, 999), ".%03d" % rnd.randint(0, 999)])
        text = "%04d-%02d-%02d%s%02d:%02d:%02d%s" % (y, mo, d, " " * rnd.randint(1, 3), h, mi, s, frac)
        m = rx.fullmatch(text)
        assert m, text
        try:
            r = dx.render_open_function_exact(text, prog)
            assert r == text, (text, r)
            ok += 1
        except ValueError:
            rej += 1  # invalid calendar/time values are rejected, never mis-rendered
    return ok, rej

if __name__ == "__main__":
    total = 0
    for t in P.TYPES:
        k = check_type(t); total += k; print("OK", t[0], k)
    print("fuzz ymd (exact, rejected):", fuzz_timestamps())
    fam = P.propose_family(7, ["2015-10-18 18:01:47,978 INFO [main] from 10.0.0.1:8080 took 12 ms, 45% of 1.5 MB, id 0x1f",
                               "2015-10-18 18:01:48,001 INFO [main] from 10.0.0.2:8081 took 7 ms, 3% of 20 MB, id 0x20"])
    print("family types:", fam["_library_types"])
    print("ALL PASS", total)
