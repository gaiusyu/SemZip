#!/usr/bin/env python3
"""Re-render the shipped validated numeric record without private run ledgers.

This checks saved-file bindings; it does not repeat the original seven-input
validation, authenticate measurements, run a codec, or reconstruct raw logs.
"""
from pathlib import Path, PurePosixPath
import argparse
import hashlib
import json
import os
import sys
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def need(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True,
                        help='Saved integration output directory, including FILES.json')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        need(args.results.is_dir(), 'Saved result directory is absent')
        need(not args.output.exists() and args.output.parent.is_dir(),
             'Output must be a new directory under an existing parent')
        manifest_path = args.results / 'FILES.json'
        manifest_sha = sha(manifest_path)
        files = json.loads(manifest_path.read_text())['files']
        observed = {}
        for name, record in files.items():
            relative = PurePosixPath(name)
            need(not relative.is_absolute() and '..' not in relative.parts,
                 'Nonrelative saved-file entry')
            path = args.results / name
            need(path.is_file() and not path.is_symlink(), 'Missing or linked saved file: ' + name)
            need(path.resolve().is_relative_to(args.results.resolve()), 'Saved file escapes result directory')
            observed[name] = sha(path)
            need(path.stat().st_size == record['bytes'] and observed[name] == record['sha256'],
                 'Saved file differs from manifest: ' + name)
        need('anonymous_results.json' in observed and 'generated/numeric_provenance.json' in observed,
             'Saved numeric inputs are not manifest-bound')
        data = json.loads((args.results / 'anonymous_results.json').read_text())
        need(data['schema'] == 'semzip.anonymous.final-results.v1', 'Unexpected saved schema')
        provenance = json.loads((args.results / 'generated/numeric_provenance.json').read_text())
        generator = Path(__file__).with_name('generate_paper_outputs.py')
        generator_sha = sha(generator)
        need(provenance['generator_sha256'] == generator_sha,
             'Renderer differs from the saved integration renderer')
        from generate_paper_outputs import generate
        with tempfile.TemporaryDirectory(prefix='saved-results-', dir=args.output.parent) as temp:
            stage = Path(temp) / 'output'
            stage.mkdir()
            generate(data, stage)
            expected_text = {name for name in files if name.startswith('generated/') and
                             Path(name).suffix in {'.tex', '.csv'}}
            actual_text = {str(p.relative_to(stage)) for p in (stage / 'generated').iterdir()
                           if p.suffix in {'.tex', '.csv'}}
            need(actual_text == expected_text, 'Rendered table/text inventory changed')
            need(all(sha(stage / name) == files[name]['sha256'] for name in expected_text),
                 'Rendered tables/text differ from the saved outputs')
            need(sha(manifest_path) == manifest_sha and sha(generator) == generator_sha and
                 all(sha(args.results / name) == digest for name, digest in observed.items()),
                 'Saved inputs or renderer changed during rendering')
            report = {
                'status': 'PASS', 'scope': 'Saved numeric re-render only; no raw measurement revalidation',
                'saved_manifest_sha256': manifest_sha,
                'anonymous_results_sha256': observed['anonymous_results.json'],
                'renderer_sha256': generator_sha, 'text_outputs_identical': len(expected_text),
                'figure_scope': 'Regenerated with installed plotting libraries; binary equality is not required',
                'output_sha256': {str(p.relative_to(stage)): sha(p) for p in sorted(stage.rglob('*')) if p.is_file()}
            }
            (stage / 'RENDER_CHECK.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
            need(not args.output.exists(), 'Output appeared during rendering')
            os.rename(stage, args.output)
        print(json.dumps({'status': 'PASS', 'text_outputs_identical': len(expected_text),
                          'scope': 'Saved numeric re-render; zero codec or model calls'}))
        return 0
    except (OSError, ValueError, KeyError, TypeError, ImportError) as exc:
        print('RENDER_FAILED: ' + str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
