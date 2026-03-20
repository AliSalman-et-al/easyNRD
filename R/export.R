#' Export a lazy NRD table to parquet
#'
#' `nrd_export()` writes a DuckDB-backed lazy table to a parquet file using
#' DuckDB `COPY`.
#'
#' @param data A DuckDB-backed lazy table.
#' @param path Output parquet path.
#'
#' @returns Invisibly returns `path`.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/file.parquet")
#'   nrd_export(data, "/path/to/out.parquet")
#' }
nrd_export <- function(data, path) {
  if (!is.character(path) || length(path) != 1 || is.na(path) || !nzchar(path)) {
    rlang::abort("`path` must be a single, non-empty file path string.")
  }

  if (!grepl("\\.parquet$", path, ignore.case = TRUE)) {
    rlang::abort("`path` must end in `.parquet`.")
  }

  .nrd_assert_duckdb_lazy(data)

  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)

  con <- .nrd_remote_con(data)
  sql <- as.character(dbplyr::sql_render(data))
  out_path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  out_sql <- as.character(DBI::dbQuoteString(con, out_path))
  copy_sql <- paste0(
    "COPY (", sql, ") TO ", out_sql,
    " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
  )

  DBI::dbExecute(con, copy_sql)
  invisible(path)
}
