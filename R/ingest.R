#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   or an existing lazy table.
#'
#' @return A lazy table, typically `tbl_dbi` backed by DuckDB.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   raw_nrd <- nrd_ingest(c("/path/to/NRD_2019_CORE.parquet", "/path/to/NRD_2020_CORE.parquet"))
#' }
#' }
nrd_ingest <- function(datasets) {
  duckdb::duckdb()

  tbl <- if (inherits(datasets, "tbl_lazy") ||
    inherits(datasets, "ArrowTabular") ||
    inherits(datasets, "arrow_dplyr_query")) {
    datasets
  } else {
    if (!is.character(datasets) || length(datasets) == 0) {
      rlang::abort("`datasets` must be a non-empty character vector of parquet paths.")
    }

    arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
      arrow::to_duckdb()
  }

  .nrd_standardize_names(tbl)
}
