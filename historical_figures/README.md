# Portable historical scientific figures

From the artifact root, with Python 3.9+, NumPy and Matplotlib installed:

```sh
python3 -B historical_figures/render.py --output ../historical-plots
```

The output directory must be new. This performs no compression, API call or model training. It renders saved numerical records into 12 PDF/PNG figures and seven point-level CSV tables. `reference_render/` retains an actual local run and its library versions/output hashes. The 11 historical figures referenced in the manuscript and supplement are covered; the append-only update-cost figure is additionally retained. Figure typography and PDF metadata need not be byte-identical across environments or to older manuscript PDFs. The PDF creation timestamps of `reference_render/` are written in UTC; `RENDER_CHECK.json` lists the hashes of the files as shipped.

`plot_inputs.json` contains all plotted points: 16 historical fixed/adaptive complete-file pairs, 144 historical throughput observations (12 files × two variants × two directions × three repeats), four program-removal pairs, four Android budget/breakdown outcomes including its failed outcome, the complete 36-training explicit-budget backbone cohort, all nine append-only datasets and seven gated-update datasets. The append-only plots show the 66 nontraining blocks from the complete 75-block ledger; nine original block-0 pairs remain in `historical_evidence/`. The gated inputs retain all 62 original blocks; plots exclude block 0 and mark future evaluation beginning at block 4. Rejected candidates' future behavior was not measured; the curves show deployed frozen outcomes.

The original plotting function bodies are adapted to package-local saved numeric inputs, public model labels and a new output directory. The exporter compared backbone, append-only and gated values to the separately packaged complete historical records, checked raw/archive arithmetic for storage/program controls, and checked the Android archive totals and semantic block sums against the retained backbone records. `NUMERIC_CHECK.json` records exact CSV content equality for all 16 storage pairs, 144 throughput observations, four program controls and four Android outcomes. Historical timers and source cohorts remain distinct from the current R71 formal benchmark.

`PROVENANCE.json` retains original audited-input and plotting-source SHA identities, without private paths or service aliases. `MANUSCRIPT_FIGURES.json` binds the original manuscript figure PDFs. The reference renderer is a numerical reproduction aid; it does not rerun the experiments or independently authenticate original decoding certificates. The original historical limitations in `historical_evidence/README.md` still apply.
