#' Materialize and Export an NRD Pipeline
#'
#' `nrd_export()` writes a processed NRD pipeline to a compressed parquet file.
#' For lazy DuckDB tables, the write is executed directly by DuckDB to preserve
#' out-of-core behavior.
#'
#' @param .data A lazy or in-memory table produced by the easyNRD pipeline.
#' @param path Output parquet file path.
#'
#' @return The output `path`, invisibly.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_export("/path/to/episodes.parquet")
#' }
#' }
nrd_export <- function(.data, path) {
  if (!is.character(path) || length(path) != 1 || is.na(path) || nchar(path) == 0) {
    rlang::abort("`path` must be a single, non-empty file path string.")
  }

  dir_path <- dirname(path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }

  if (inherits(.data, "tbl_lazy") || inherits(.data, "arrow_dplyr_query")) {
    con <- tryCatch(dbplyr::remote_con(.data), error = function(e) NULL)

    if (!inherits(con, "duckdb_connection")) {
      rlang::abort(
        paste(
          "`nrd_export()` requires a DuckDB-backed lazy table for out-of-core export.",
          "Use `nrd_ingest()` to initialize the pipeline backend."
        )
      )
    }

    query_sql <- as.character(dbplyr::sql_render(.data))
    out_path <- normalizePath(path, winslash = "/", mustWork = FALSE)
    out_path_sql <- as.character(DBI::dbQuoteString(con, out_path))
    copy_sql <- paste0(
      "COPY (", query_sql, ") TO ", out_path_sql,
      " (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    DBI::dbExecute(con, copy_sql)
    return(invisible(path))
  }

  arrow::write_parquet(.data, sink = path, compression = "zstd")
  invisible(path)
}
