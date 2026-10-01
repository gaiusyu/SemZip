#!/usr/bin/env python3
"""R76-B2: run the unchanged R69 trainer compressor (semzip_pure.py) with its LLM call replaced by the
LIBRARY-V1 proposer.  Only two things are patched, in memory:
  * semzip_pure.call_llm            -> deterministic library proposer (no network, api_calls stays 0);
  * build_family_batch_prompt / build_prompt are wrapped (their output is returned unchanged) only to
    remember which family ids/examples each prompt carries, so the proposer sees exactly the examples
    the LLM would have seen.
Verifier-repair prompts (the only other call_llm use) get an empty function list: the fixed library has
no repair capability ("Return an empty list if unrepairable" is a valid answer under the repair prompt).
urllib.request.urlopen is replaced by a function that raises, so any accidental API attempt fails loudly.
Invoked by train_lib.py in place of semzip_pure.py with the identical command line."""
import json, os, sys, threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
TRAINER = Path(os.environ["R76_LIB_TRAINER_DIR"])
sys.path.insert(0, str(TRAINER))
sys.path.insert(0, str(HERE))

import urllib.request  # noqa: E402


def _no_network(*_a, **_k):
    raise RuntimeError("R76-B2 library run: network access is forbidden")


urllib.request.urlopen = _no_network

import proposer  # noqa: E402
import semzip_pure as sp  # noqa: E402

_REG = {}
_LOCK = threading.Lock()
_orig_batch_prompt = sp.build_family_batch_prompt
_orig_prompt = sp.build_prompt


def _max_examples(args, kwargs, pos):
    return int(kwargs["max_examples"] if "max_examples" in kwargs else args[pos])


def build_family_batch_prompt(*args, **kwargs):
    prompt = _orig_batch_prompt(*args, **kwargs)
    family_examples = args[0] if args else kwargs["family_examples"]
    k = _max_examples(args, kwargs, 2)
    entry = ("batch", [(int(f.family_id), [str(e) for e in ex[:k]]) for f, ex in family_examples])
    with _LOCK:
        _REG[prompt] = entry
    return prompt


def build_prompt(*args, **kwargs):
    prompt = _orig_prompt(*args, **kwargs)
    family = args[0] if args else kwargs["family"]
    k = _max_examples(args, kwargs, 2)
    entry = ("single", [(int(family.family_id), [str(e) for e in family.examples[:k]])])
    with _LOCK:
        _REG[prompt] = entry
    return prompt


def call_llm(prompt, args, cache_path):
    cache_path = Path(cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    stats = sp.get_llm_runtime_stats(args)
    with _LOCK:
        stats["proposal_events"] = int(stats.get("proposal_events", 0)) + 1
        entry = _REG.get(prompt)
    if entry is None:
        payload = {"functions": [], "_proposal_cache_mode": "library_v1_no_repair"}
        key = "library_repair_declined"
    elif entry[0] == "batch":
        payload = proposer.propose_batch(entry[1])
        payload["_proposal_cache_mode"] = "library_v1"
        key = "library_batch_proposals"
    else:
        payload = proposer.propose_family(*entry[1][0])
        payload["_proposal_cache_mode"] = "library_v1"
        key = "library_single_proposals"
    with _LOCK:
        stats[key] = int(stats.get(key, 0)) + 1
    cache_path.with_suffix(".prompt.txt").write_text(prompt, encoding="utf-8")
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return payload


sp.build_family_batch_prompt = build_family_batch_prompt
sp.build_prompt = build_prompt
sp.call_llm = call_llm

if __name__ == "__main__":
    rc = sp.main()
    sys.exit(rc)
