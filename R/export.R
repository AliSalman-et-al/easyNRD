#' Materialize and Export an NRD Pipeline
#'
#' `nrd_export()` writes a processed NRD pipeline to a compressed parquet file.
#' For lazy DuckDB tables, the write is executed directly by DuckDB to preserve
#' out-of-core behavior.
#'
#' @param .data A DuckDB-backed lazy table produced by the easyNRD pipeline.
#' @param path Output parquet file path.
#' @param row_group_size Target parquet row-group size used during DuckDB
#'   materialization. Smaller groups reduce peak memory pressure for wide NRD
#'   extracts. Defaults to `250000L`.
#'
#' @returns The output `path`, invisibly.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_export("/path/to/episodes.parquet", row_group_size = 250000L)
#' }
#' }
nrd_export <- function(.data, path, row_group_size = 250000L) {
  if (!is.character(path) || length(path) != 1 || is.na(path) || nchar(path) == 0) {
    rlang::abort("`path` must be a single, non-empty file path string.")
  }

  if (!is.numeric(row_group_size) || length(row_group_size) != 1 || is.na(row_group_size) || row_group_size <= 0) {
    rlang::abort("`row_group_size` must be a single positive numeric or integer value.")
  }
  row_group_size <- as.integer(row_group_size)
  if (row_group_size <= 0) {
    rlang::abort("`row_group_size` must be greater than 0.")
  }

  dir_path <- dirname(path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }

  .nrd_assert_lazy_duckdb(.data, arg = ".data")

  con <- dbplyr::remote_con(.data)
  query_sql <- as.character(dbplyr::sql_render(.data))
  out_path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  out_path_sql <- as.character(DBI::dbQuoteString(con, out_path))
  copy_sql <- paste0(
    "COPY (", query_sql, ") TO ", out_path_sql,
    " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ", row_group_size, ")"
  )

  DBI::dbExecute(con, copy_sql)
  invisible(path)
}
