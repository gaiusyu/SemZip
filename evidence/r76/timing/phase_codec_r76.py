#!/usr/bin/env python3
"""Codec-phase entry for the R76 generic codecs (xz9e, zstd19). usage: phase_codec_r76.py encode|decode REQUEST.json

The analogue of r68_external_20260923/formal_campaign_v3.py `codec-phase` (which times the R68 codecs), but
using vendor/codec_baseline_r76.py, a byte-identical copy of ../codecs/codec_baseline_r76.py (sha256 pinned in timing_r76.py,
checked by timing_r76.py). Fresh process per phase; consumes the pinned codec spec without timing hash/version
audits; encode_all/decode_all are the runner's own (exact 100,000-line blocks, 4 block workers, one CLI process
with -T1 per block, archive-only decode into one complete restored file).
"""
import sys
sys.dont_write_bytecode = True
import json
import os
from pathlib import Path

H = Path(__file__).resolve().parent
sys.path.insert(0, str(H / 'vendor'))
import codec_baseline_r76 as cb  # noqa: E402

WORKERS = 4
TIMEOUT = 3600


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):  # = formal_campaign_v3.save
    path = Path(path)
    temporary = path.with_name(path.name + '.pending.%d' % os.getpid())
    with temporary.open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
    os.replace(str(temporary), str(path))


def main():
    phase, request = sys.argv[1], load(sys.argv[2])
    codec = cb.Codec.__new__(cb.Codec)
    codec.name, codec.spec = request['method'], request['codec_spec']
    codec.dictionary_mode = 'dictionary_file' in codec.spec
    codec.encoder = codec.decoder = Path(codec.spec['executable'])
    attempt = Path(request['attempt'])
    if phase == 'encode':
        inv = request['inventory']
        blocks, seconds, extra = cb.encode_all(inv['source']['path'], inv, codec, request['dataset'], attempt, WORKERS, TIMEOUT)
        save(attempt / 'encode_phase.json', {'blocks': blocks, 'inner_encode_seconds': seconds, 'extra': extra})
    elif phase == 'decode':
        encoded = load(attempt / 'encode_phase.json')
        restored, seconds = cb.decode_all({}, codec, request['dataset'], attempt, encoded['blocks'], WORKERS, TIMEOUT)
        save(attempt / 'decode_phase.json', {'restored': str(restored), 'inner_decode_seconds': seconds})
    else:
        raise SystemExit(__doc__)


if __name__ == '__main__':
    main()
