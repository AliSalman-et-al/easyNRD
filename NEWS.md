# easyNRD 0.0.0.9000

- Added a developer benchmark harness at `data-raw/benchmarks/benchmark_pipeline.R` for staged and end-to-end pipeline timing on synthetic NRD-scale parquet fixtures, including a `profvis` profile for `nrd_link_readmissions()`.
- Narrowed the internal `nrd_link_readmissions()` linkage checkpoint to materialize only join-critical columns plus requested `readmit_vars`, while preserving the full denominator and original output column surface.
