> **Note (repository version).** This is the README of the SemZip-1 base package (single greedy synthesis per dataset),
> kept for its detailed replay contract. The top-level `README.md` supersedes it. `replay.py` now defaults to the
> paper's main deployments; add `--deployment-set semzip1` to the `replay.py` commands below to replay SemZip-1.
> In this repository the 16 training samples (`offline_evidence/<Dataset>/sample.log`) are not shipped; rebuild them
> from the fetched LogHub files with `offline_evidence/tools/rebuild_samples.py`.

# SemZip R71 anonymous reproduction artifact

**Unpublished review package.** This package supports offline replay of the
sixteen frozen programs and policies. `results/final/` contains all 96 complete-file
outcomes and 216 formal observations, validated against the seven original
terminal inputs. Every formal archive matches its accepted first pass.

## Requirements

- Linux x86-64, Python 3.9 or newer, `libarchive`, `libpcre2-8`, and XZ.
- The frozen Linux binaries are included with their original SHA-256 values.
- Online replay needs no Python package, model service, credential or network.
- Optional figure generation needs Matplotlib and NumPy. Sanitized original
  prompts, responses, samples and compiler traces are included as offline
  evidence; transport records and private API configuration are excluded.

For Debian/Ubuntu, dependency packages are `python3 libarchive13 libpcre2-8-0
xz-utils`; rebuilding additionally needs `g++ libarchive-dev libpcre2-dev`.
Generated programs use UTC0 and the C locale, both recorded in each semantic
archive. A Unix `time.tzset` implementation is required.

## One complete dataset

Download the Linux archive using the exact link in
`metadata/raw_datasets.json`, or let this command download it:

```sh
python3 scripts/prepare_data.py --dataset Linux \
  --archive downloads/Linux.tar.gz --raw-dir raw --download
python3 replay.py verify-input --dataset Linux --input raw/Linux.log
python3 replay.py roundtrip --dataset Linux --input raw/Linux.log \
  --result runs/linux --workers 4
```

The result directory must be new. Roundtrip writes complete native and semantic
archives, then starts a separate archive-only decoder process. A second read of
the materialized output must match the original full-file SHA. The external
storage policy is deliberately unavailable to that decoder. Input validation is
outside the diagnostic process timer; internal guards and hashes remain timed.

Decode an existing archive without the input file or program:

```sh
python3 replay.py decode --archive runs/linux/encode/archive \
  --output recovered/Linux.log --result runs/linux-decoder --workers 4
```

## Fixed contract

- Exactly 100,000 original physical LF records per block, including the original
  tail. No normalization, sampling, truncation, or dataset-specific block size.
- Programs and storage policies are fixed offline using original block 0 only.
  A file no larger than one block has no held-out suffix.
- No cross-block value dictionaries, deltas, aliases or data memoization.
  Data caches and callable globals are cleared at every block boundary and
  before the actual semantic archive inverse check.
- Local validation catches exceptions and leaves that individual match unchanged,
  including caught memory/I/O exceptions. A recognized semantic block failure
  takes one fixed empty-program / empty-storage recovery path. Infrastructure,
  source or unexpected errors that propagate to the block guard, failed recovery,
  and native failures abort. These are distinct handling scopes.
- Full archive costs include all native payloads, semantic payloads and the
  decoding manifest. Diagnostic logs are not decoder inputs.

## Contents and provenance

| Location | Purpose |
|---|---|
| `source/` | 120 unchanged files from the frozen source snapshot, including native binaries |
| `frozen/` | The three unchanged guard/helper files and original R71 campaign source |
| `deployments/` | Sixteen byte-identical frozen program/policy pairs |
| `metadata/` | Relative-path hashes, exact raw data/member identities, sanitized training counts |
| `replay.py` | New portable entry point; does not change frozen source bytes |
| `scripts/` | Data reconstruction, package verification and report interfaces |
| `external/` | Public baseline commits, adapters, patches and setup references |
| `docs/` | Reproduction boundaries and result schema |
| `offline_evidence/` | Identities of all 16 samples (rebuilt by `tools/rebuild_samples.py`), 202 sanitized request/response pairs, compiler traces and public configuration |
| `historical_figures/` | All historical manuscript plots from portable numeric inputs, with point tables and an actual reference rendering |
| `historical_evidence/` | Separate complete-row historical backbone/evolution evidence, with its own protocol limits |
| `results/suffix/` | Complete matched nontraining suffix archive costs, block certificates and rerenderable TeX |
| `integration/` | Strict final-result validator and portable numerical renderers |
| `results/final/` | All validated current complete-file/formal observations, tables and figures |
| `validation_controls/` | Cold observer equality and complete, separately scoped response-boundary controls |
| `source_study_evidence/` | Historical source census and paired-field motivation records, separate from strict deployment |

Nine unused machine-specific legacy launchers/documentation files are omitted;
their original hashes remain in the full 129-file provenance inventory. Included
files keep their original bytes. `metadata/source_provenance.json` distinguishes original source hashes from the
new portable wrapper. `MANIFEST.json` inventories package bytes. The original
`frozen/main_campaign.py` is retained for provenance, not as the portable entry
point: its original project layout is deliberately not emulated. Historical
profiles and API variable names inside unchanged source are not the R71 replay
configuration; only `metadata/deployments.json` selects deployed programs.

## Rebuild without relabeling binary hashes

On an incompatible Linux environment, copy `source/` to a new directory and run
`build.sh` inside its `backend/` directory. Then supply that source copy:

```sh
cp -a source rebuilt-source
(cd rebuilt-source/backend && bash build.sh)
python3 replay.py roundtrip --dataset Linux --input raw/Linux.log \
  --source-root rebuilt-source --allow-rebuilt-binaries \
  --result runs/linux-rebuilt
```

Every nonbinary source file must still match. Rebuilt ELF binaries receive
their actual new hashes in the run report; no claim of original binary identity
is made. Verify losslessness and archive equivalence before comparing results.

## Results and scope

The included Linux reference is a size/correctness smoke target, not formal
throughput evidence. Diagnostic replay durations must not be mixed with the
paper's separately scheduled repeated process-wall experiment. See
`docs/RESULT_SCHEMA.md` for the final table/figure input interface and
`docs/DATA.md` for exact public data assembly. This is finite validation of
specific inputs and programs, not a proof for arbitrary generated code.

Full raw datasets and private service configurations are not redistributed.
The 16 saved training samples are rebuilt from the public logs (see the note at the top). Public datasets and third-party code retain their original licensing/citation terms.
This working package is not a public release and does not assert a new
redistribution license for the complete collection.

## Distinct verification scopes

1. **Frozen-program deployment:** the portable Linux control independently
   reconstructs the complete original file from its counted archive, with the
   deployment policy unavailable to the decoder. This is finite correctness
   and portability evidence, not formal throughput or all-dataset portability.
2. **Original training evidence:** `offline_evidence/` preserves the identities of the 16 exact sample
   files (rebuilt from the public logs), 202 sanitized request/response pairs (1,238,005 observed tokens) and
   all compiler records. Pairing, counts, content and sample identities were
   checked. A separate local response-content boundary control reproduces 14/16
   exact plan files under UTC0/C; Linux/Mac differ. Two separately labeled
   Asia/Shanghai diagnostics reproduce those plans and explain all 28 changed
   repair counterexamples. The original training timezone was not pinned or
   captured. These are not a uniform 16-file UTC0 success, HTTP/provider replay,
   or complete sampling/network/fitting replay. The package-relative response
   replay suite reproduces all eighteen stated outcomes directly from the
   included source and evidence; see `validation_controls/fresh_response_boundary/README.md`.
   Sanitized transport records are
   not byte-identical originals, and model aliases do not pin a served implementation.
3. **External archive capability:** the supplementary directory-only decoder
   control passes eight fixtures / 11 blocks with no encoding. The frozen
   formal wrapper still uses its reconstructible encode-phase index; see
   `external/archive_directory_decode_control/README.md` for that qualification.

Included final main and formal results passed `integration/integrate_results.py`
on the actual terminal inputs. The validator retains failed cells/trials, requires complete
fixed inventories and exact evidence identities, and refuses missing inputs.
Historical backbone/evolution records remain separate from the strict final
main protocol and are not silently treated as repetitions of it.

The R67 cold-observer control additionally retains exact equality of all 14 saved observations and seven trigger decisions under explicit data/callable-cache reset. It does not retroactively claim that the entire historical protocol used the R71 reset implementation. See `validation_controls/README.md`.

The source-study subtree retains Q1/Q2 historical records and metadata checks. Its whole-file delta/varint diagnostic is separate from the strict 100,000-record deployment experiments. Recorded row labels have no established human-reviewer attribution; the package does not claim otherwise.

## Reproduce the saved result presentation

Run `python3 -B integration/render_saved_results.py --results results/final --output ../regenerated-results` with a new output directory. The actual packaged-data check reproduced all 22 TeX/CSV outputs exactly; `metadata/FINAL_RESULT_REPRODUCTION.json` binds that run. Regenerated figures depend on installed plotting-library versions. See `historical_figures/README.md` for all historical plots and `results/suffix/README.md` for the separate nontraining suffix cost view. These commands do not repeat model calls or compression experiments.
