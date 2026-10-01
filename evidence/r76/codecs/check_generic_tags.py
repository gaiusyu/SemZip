#!/usr/bin/env python3
"""Read-only post-run check for DeLog-generic archives.

Official DeLog's decompressor has dataset-name-keyed special handlers that fire only for
the bare regex tags <T>,<I>,<P>,<O>,<Q>,<R>,<X>,<Y>,<Z>,<A>,<B>,... produced by the
dataset-name regex_map.  The generic build has an empty regex_map, so none of its
archives may contain such a tag in tags_mapping.txt.  This script counts them in every
(or every Nth) block archive of the latest delog_generic attempt per dataset.

usage: check_generic_tags.py CAMPAIGN_DIR [--every N] [--codec delog_generic]
"""
import argparse
import re
import tarfile
from pathlib import Path

BARE = re.compile(r":<[A-Z]>$")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("campaign", type=Path)
    p.add_argument("--every", type=int, default=1)
    p.add_argument("--codec", default="delog_generic")
    a = p.parse_args()
    bad_total = 0
    for ds_dir in sorted(x for x in a.campaign.iterdir() if (x / a.codec).is_dir()):
        attempts = sorted((ds_dir / a.codec / "trial_001").glob("attempt_*"))
        if not attempts:
            continue
        archives = sorted((attempts[-1] / "archives").glob("block_*.tar.xz"))[::a.every]
        bad = 0
        for arc in archives:
            with tarfile.open(str(arc), "r:xz") as t:
                member = next(m for m in t.getmembers() if m.name.lstrip("./") == "tags_mapping.txt")
                text = t.extractfile(member).read().decode("utf-8", "replace")
            bad += sum(1 for line in text.splitlines() if BARE.search(line))
        bad_total += bad
        print("%s\t%s\tarchives_checked=%d\tbare_regex_tags=%d" % (ds_dir.name, attempts[-1].name, len(archives), bad))
    print("TOTAL_bare_regex_tags=%d" % bad_total)


if __name__ == "__main__":
    main()
