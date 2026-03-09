#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   an Arrow query, or an existing DuckDB-backed lazy table.
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
  tbl <- if (inherits(datasets, "tbl_lazy")) {
    .nrd_assert_lazy_duckdb(datasets, arg = "datasets")
    datasets
  } else if (inherits(datasets, "ArrowTabular") ||
    inherits(datasets, "arrow_dplyr_query")) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    arrow::to_duckdb(datasets, con = con)
  } else {
    if (!is.character(datasets) || length(datasets) == 0) {
      rlang::abort("`datasets` must be a non-empty character vector of parquet paths.")
    }

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
      arrow::to_duckdb(con = con)
  }

  .nrd_assert_lazy_duckdb(tbl, arg = "datasets")
  .nrd_standardize_names(tbl)
}
