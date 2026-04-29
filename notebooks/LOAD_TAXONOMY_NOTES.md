# load_taxonomy_summaries.ipynb — lessons from the build-out

Notes captured while iterating on issue #77 (load NOW-tier NMDC taxonomy summaries).
Most lessons are transferable to other on-pod loaders.

## Architecture in one paragraph

Pull the URL manifest from the BERDL Hive catalog over **Spark Connect** (`get_spark_session()` is auto-imported on the pod). Dedupe and group by `data_object_type`. For each type, fetch every URL in parallel from `data.microbiomedata.org` (HTTP, no auth needed for downloads), cache raw text on disk, parse per-format, concat into a single pandas DataFrame, write one Parquet per type. Each Parquet carries `workflow_run_id` (`= was_generated_by`) and `data_object_id` for downstream joins.

## On-pod Spark + auth (the biggest gotcha)

- **Use the auto-imported `get_spark_session()`**. The BERDL kernel image runs `~/.ipython/profile_default/startup/00-notebookutils.py` which imports the helper and `03-spark-connect-server.py` which starts a per-kernel Spark Connect server. **Do not** import `from pyspark.sql import SparkSession` and build the URI yourself — you'll fight auth all day.
- The Spark Connect server requires the KBase token in an `x-kbase-token` (or `Authorization: Bearer …`) gRPC header. The helper builds the URI as `sc://host:15002/;x-kbase-token=$KBASE_AUTH_TOKEN`.
- The token lives in `~/.berdl_kbase_session` and is mirrored into `os.environ["KBASE_AUTH_TOKEN"]` by `05-token-sync.py` (background daemon, refresh every 30s).
- Don't go through the REST/MCP at `https://hub.berdl.kbase.us/apis/mcp` for big queries on the pod — it paginates with `LIMIT/OFFSET` (O(N²) page work) and caps `limit` server-side at 1000. ~15K rows took minutes via REST vs **2.5 seconds via Spark Connect**.

## Jupyter workflow gotchas

- **"File → Reload Notebook from Disk" updates the editor only.** The kernel still runs whatever you last executed. After any cell edit you must re-execute that cell to update kernel state. The cleanest reset is **Kernel → Restart Kernel** + Run All.
- Add a **fail-fast sanity check** at the top of expensive cells:
  ```python
  import inspect
  assert "marker_string_from_latest_version" in inspect.getsource(my_function), \
      "Kernel has STALE my_function — re-run its defining cell."
  ```
  This caught several "stale kernel" cycles that would otherwise burn 5 minutes downloading before failing.
- When iterating, watch for **duplicate cells** in the file. NotebookEdit's `cell_id` is positional in the Read output, not stable in the JSON. Verify with `python -c "import json; nb = json.load(open('foo.ipynb')); ..."` rather than assuming the edit landed where intended.

## NMDC data quirks discovered

### Placeholder files instead of empty files
Many workflow runs that produced no results emit a single-line file like:

| Type | Placeholder content |
|---|---|
| CheckM | `No Checkm Results for nmdc:wfmag-...` |
| GOTTCHA2 | `Nothing found in gottcha2 for nmdc:... 2026-04-18T...` (with timestamp) |
| GTDBTK Archaeal | `No Archaeal Results for nmdc:wfmag-...` |
| GTDBTK Bacterial | `No Bacterial Results for nmdc:wfmag-...` |
| Kraken2 | `Nothing found in kraken2 for nmdc:...` (speculative pattern, but our parsers handle it) |

If you let `pd.read_csv` parse these, the placeholder line becomes a *unique column header* — concat-ing 3,535 of them yields a 0-row × 3,535-column DataFrame that takes forever to write. **Always detect placeholders up front and return `None` so the row is excluded from concat.** See `_is_placeholder()` in cell 7.

### Zero archaeal MAGs across 3,535 runs
**Every single GTDBTK Archaeal file in the manifest is a placeholder.** Confirmed 3,337/3,337 cached files are exactly 49 bytes. Bacterial files in the same workflow runs have real data (avg ~120KB). Either NMDC's binning pipeline filters out archaeal bins before GTDB-Tk, or none of the assemblies in this corpus recovered any. Worth raising upstream if you expected archaea.

### Duplicate URLs in `data_object_set` (MAG types only)

| Type | Manifest rows | Unique URLs | Duplicates |
|---|---|---|---|
| GOTTCHA2 | 4,432 | 4,432 | 0 |
| Kraken2 | 4,432 | 4,432 | 0 |
| CheckM | 3,535 | 3,337 | **198** |
| GTDBTK Archaeal | 3,535 | 3,337 | **198** |
| GTDBTK Bacterial | 3,535 | 3,337 | **198** |

Same workflow runs are duplicated across all three MAG-derived types (e.g. `nmdc:wfmag-11-acysmk94.4`). Each duplicate URL has 2–3 distinct `nmdc:dobj-...` ids. Likely cause: MAG workflow was re-ingested without replacing prior `data_object` records. **Always dedup on `(url, data_object_type)` before processing** — duplicates cause cache-write races and double the parsing work.

### 8 broken URLs per *bt-type*
GOTTCHA2 and Kraken2 each have 8 manifest URLs that 404 on `data.microbiomedata.org` (same workflow run IDs in both, so it's the workflow output that's missing, not type-specific). Real upstream gap; surface in error log and move on.

## Format-parsing notes

- **CheckM `_qa.out`**: looks like a fixed-width table but isn't safe for `read_fwf`. Has `---` separator lines bracketing header and data. Column names contain spaces (`Bin Id`, `Marker lineage`, `# genomes`, …). Total = **14 columns** (including `5+` count column). Split on `\s{2,}` so `marker_lineage` (e.g. `k__Bacteria (UID203)` — single space inside) survives. Skip the literal `Bin Id` header row by exact-match (NOT `startswith("bin")` — that swallows data rows like `bins.1`).
- **GOTTCHA2 TAXID**: mixed int/str across files. Real values include `"2"` (Bacteria), `"2759_pt"` (Eukaryota plastid). Concat → object dtype → pyarrow can't infer schema → **must coerce to string**.
- **GTDBTK summaries**: column name varies (`user_genome` in some files, `name` in others) — normalize to `name` in the parser.
- **Kraken2**: no header, fixed 6 columns. Use `names=` and consistent dtypes.

## Performance levers that mattered

| Lever | Before | After |
|---|---|---|
| Manifest fetch | minutes (REST + LIMIT/OFFSET pagination) | **2.5s** (Spark Connect) |
| File downloads | sequential | **32 parallel threads** (`ThreadPoolExecutor`) |
| Re-runs | re-download every file | **disk cache** (`OUT_DIR/raw_cache/`, mirrors URL path) |
| Race on duplicate URLs | `.tmp` collisions | **per-thread temp suffix** (`{path.suffix}.{thread_id}.tmp`) |
| Stale-kernel debug cycles | 5 min per cycle | **<1s** (assert sanity check at top of main loop) |

After the first cache-warm run, all 19K files re-process in ~4 minutes (mostly Kraken2 size); a fully-cached re-run with parser tweaks takes well under a minute.

## Output and what to do with it

Five Parquets in `loaded_taxonomy/`:

| File | Rows | Notes |
|---|---|---|
| `checkm_statistics.parquet` | 70,211 | Real ints/floats |
| `gottcha2_classification_report.parquet` | 673,049 | All `string` (TAXID forces it) |
| `gtdbtk_archaeal_summary.parquet` | — not written — | All inputs are placeholders |
| `gtdbtk_bacterial_summary.parquet` | 18,601 | Real floats |
| `kraken2_classification_report.parquet` | 36,096,235 | Real ints/floats |

Each row carries `workflow_run_id` and `data_object_id`. To go from workflow run → biosample, join through `nmdc_metadata.workflow_execution_set.was_informed_by` → omics processing → `has_input` → biosample.

## Files in this directory

- `load_taxonomy_summaries.ipynb` — the notebook
- `loaded_taxonomy/` — outputs
  - `*.parquet` — final tables
  - `raw_cache/` — raw HTTP downloads, keyed by URL path. Safe to delete to force re-fetch
  - `raw_samples/` — first 2 raw files per type, kept for parser debugging
  - `logs/run_<timestamp>.log` — one per run, full file logging via Python `logging`

## Useful commands

```bash
# Fast inspection of a parquet
python -c "import pandas as pd; print(pd.read_parquet('loaded_taxonomy/checkm_statistics.parquet').dtypes)"

# Cache stats
find loaded_taxonomy/raw_cache -type f | wc -l
du -sh loaded_taxonomy/raw_cache

# Latest run log
ls -t loaded_taxonomy/logs/ | head -1
```
