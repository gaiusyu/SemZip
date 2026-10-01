#!/usr/bin/env python3
"""Verify shipped Spark control records only; no large input reads or network."""
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    root = Path(__file__).resolve().parent
    artifact = root.parents[1]
    read = lambda name: json.loads((root / name).read_text())
    provenance = read('PROVENANCE.json')
    assert len(provenance['records']) == 4
    for row in provenance['records']:
        name = Path(row['export_file'])
        assert not name.is_absolute() and '..' not in name.parts
        path = root / name
        assert sha(path) == row['export_sha256']
        assert path.stat().st_size == row['export_bytes']
        if row['byte_identical']:
            assert row['original_sha256'] == row['export_sha256']
            assert row['original_bytes'] == row['export_bytes']
    result = read('summary.json')
    assert result['status'] == 'PASS'
    reference_path = artifact / 'metadata/raw_datasets.json'
    reference = next(r for r in json.loads(reference_path.read_text())['datasets']
                     if r['dataset'] == 'Spark')
    historical = next(r for r in json.loads((root.parent / 'q2/run_manifest.json').read_text())['inputs']
                      if r['dataset'] == 'Spark')
    assert sha(reference_path) == result['inputs']['reference_ledger_sha256']
    assert sha(root / 'verify_spark_assembly.py') == result['script_sha256']
    a = result['inputs']['archive']
    assert a['reference_identity_pass'] is True
    assert (a['bytes'], a['sha256'], 'md5:' + a['md5']) == (
        reference['archive_bytes'], reference['archive_sha256'], reference['official_checksum'])
    h = result['inputs']['historical']
    assert h['identity_pass'] is True
    assert h['bytes'] == h['expected_bytes'] == historical['raw_bytes']
    assert h['sha256'] == h['expected_sha256'] == historical['raw_sha256']
    actual = read('actual_archive_members.json')
    boundaries = read('historical_member_boundary_checks.json')['rows']
    names = [r['path'] for r in reference['members']]
    assert len(names) == len(set(names)) == len(boundaries) == result['member_count'] == 3852
    assert result['reference_member_count'] == 3852
    assert names == sorted(names) == actual['tar_order'] == [r['path'] for r in actual['members']]
    assert len(actual['members']) == 3852
    for i, (expected, member, boundary) in enumerate(zip(reference['members'], actual['members'], boundaries)):
        assert all(member[k] == expected[k] for k in ['path', 'bytes', 'sha256', 'newline_count', 'ends_with_lf'])
        assert member['reference_pass'] is True and member['ends_with_lf'] is True
        assert boundary['index'] == i and boundary['path'] == member['path']
        assert boundary['observed_payload_bytes'] == member['bytes']
        assert boundary['observed_payload_sha256'] == member['sha256']
        assert boundary['matches_actual_tar_member'] is True
        assert boundary['observed_separator_hex'] == '0a'
        assert boundary['separator_is_single_lf'] is True
    plain = sum(r['bytes'] for r in actual['members'])
    assert plain == reference['raw_bytes']
    assert plain + len(names) == historical['raw_bytes']
    assert result['historical_extra_bytes'] == 0 and result['unexpected_archive_entries'] == []
    for name in ['tar_order_is_lexical', 'member_set_pass', 'every_tar_member_hash_matches_reference',
                 'historical_verified_member_and_single_lf_mapping', 'source_input_stat_unchanged',
                 'byte_identical_public_archive_reconstruction_recipe_verified']:
        assert result[name] is True
    candidates = result['candidates']
    assert len(candidates) == 2
    for row, expected_bytes, expected_sha in zip(candidates, [plain, plain + len(names)],
                                                [reference['sha256'], historical['raw_sha256']]):
        assert row['match'] is True and row['verified_tar_payload_binding'] is True
        assert row['candidate_source'] == 'direct lexical tar member stream'
        assert row['bytes'] == row['expected_bytes'] == expected_bytes
        assert row['sha256'] == row['expected_sha256'] == expected_sha
    assert result['remote_payload_downloaded_or_revalidated'] is False
    assert result['original_historical_assembly_script_found'] is False
    assert all(result[k] == 0 for k in ['network_accesses', 'api_calls', 'compression_experiments',
                                      'assembled_output_files_created'])
    print(json.dumps({'status': 'PASS_SAVED_CONTROL_METADATA', 'members': len(names),
                      'historical_boundaries': len(boundaries), 'whole_file_candidates': 2,
                      'scope': 'Recorded content-hash evidence and arithmetic only; no raw archive or historical file reread.'}))


if __name__ == '__main__':
    main()
