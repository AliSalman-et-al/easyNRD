#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   an Arrow query, or an existing DuckDB-backed lazy table.
#' @param temp_directory Optional DuckDB spill directory used for temporary
#'   on-disk operations. Provide a single path string (for example, a fast local
#'   SSD directory) to reduce RAM pressure during large window operations.
#' @param memory_limit Optional DuckDB memory limit as a single string, such as
#'   `"8GB"`.
#'
#' @section Hardware Optimizations:
#' For best performance and stability on constrained hardware, pre-process raw
#' parquet files so they are partitioned by `YEAR` and sorted by
#' `NRD_VISITLINK` then `NRD_DAYSTOEVENT`. This layout helps DuckDB leverage
#' parquet row-group min/max statistics to skip irrelevant blocks and minimize
#' expensive external sorting.
#'
#' @returns A lazy table, typically `tbl_dbi` backed by DuckDB.
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   raw_nrd <- nrd_ingest(
#'     c("/path/to/NRD_2019_CORE.parquet", "/path/to/NRD_2020_CORE.parquet"),
#'     temp_directory = "/fast_scratch/duckdb_tmp",
#'     memory_limit = "8GB"
#'   )
#' }
#' }
nrd_ingest <- function(datasets, temp_directory = NULL, memory_limit = NULL) {
  if (!is.null(temp_directory) &&
    (!is.character(temp_directory) || length(temp_directory) != 1 || is.na(temp_directory) || nchar(temp_directory) == 0)) {
    rlang::abort("`temp_directory` must be NULL or a single, non-empty string.")
  }

  if (!is.null(memory_limit) &&
    (!is.character(memory_limit) || length(memory_limit) != 1 || is.na(memory_limit) || nchar(memory_limit) == 0)) {
    rlang::abort("`memory_limit` must be NULL or a single, non-empty string like '8GB'.")
  }

  configure_duckdb <- function(con) {
    if (!is.null(temp_directory)) {
      temp_directory_sql <- as.character(DBI::dbQuoteString(con, temp_directory))
      DBI::dbExecute(con, paste0("PRAGMA temp_directory=", temp_directory_sql))
    }

    if (!is.null(memory_limit)) {
      memory_limit_sql <- as.character(DBI::dbQuoteString(con, memory_limit))
      DBI::dbExecute(con, paste0("PRAGMA memory_limit=", memory_limit_sql))
    }

    con
  }

  tbl <- if (inherits(datasets, "tbl_lazy")) {
    .nrd_assert_lazy_duckdb(datasets, arg = "datasets")
    datasets
  } else if (inherits(datasets, "ArrowTabular") ||
    inherits(datasets, "arrow_dplyr_query")) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    con <- configure_duckdb(con)
    arrow::to_duckdb(datasets, con = con)
  } else {
    if (!is.character(datasets) || length(datasets) == 0) {
      rlang::abort("`datasets` must be a non-empty character vector of parquet paths.")
    }

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    con <- configure_duckdb(con)
    arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
      arrow::to_duckdb(con = con)
  }

  .nrd_assert_lazy_duckdb(tbl, arg = "datasets")
  .nrd_standardize_names(tbl)
}
