# easyNRD development news

## easyNRD 0.0.0.9000

- Breaking change: removed deprecated compatibility wrappers `nrd_read()` and
  `nrd_link()`. Use `nrd_ingest()`, `nrd_build_episodes()`, and
  `nrd_link_readmissions()`.
- Updated documentation to reflect the current tidy pipeline and modern roxygen
  conventions.
- `nrd_build_episodes()`, `nrd_factorize()`, and `nrd_link_readmissions()` now
  use `.data` as the primary data argument.
