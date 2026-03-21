# easyNRD 0.0.0.9000

- Added a developer benchmark harness at `data-raw/benchmarks/benchmark_pipeline.R` for staged and end-to-end pipeline timing on synthetic NRD-scale parquet fixtures, including a `profvis` profile for `nrd_link_readmissions()`.
- Narrowed the internal `nrd_link_readmissions()` linkage checkpoint to materialize only join-critical columns plus requested `readmit_vars`, while preserving the full denominator and original output column surface.
- Broke `nrd_link_readmissions()` first-readmission ranking into sequential checkpointed stages so the `gap`, `DTE_cand`, and final key tie-break passes no longer stack multiple blocking window operators in a single DuckDB plan.
- Added optional environment- and option-driven DuckDB thread and memory settings during `nrd_ingest()`, plus `EASYNRD_VERBOSE=1` checkpoint logging for linkage profiling.
