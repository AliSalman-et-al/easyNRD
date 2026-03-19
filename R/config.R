#' Configure DuckDB Engine Pragmas for easyNRD Pipelines
#'
#' `nrd_configure_engine()` applies DuckDB runtime pragmas to an active
#' connection. Use this to configure memory use, temporary spill location, and
#' worker thread allocation before running large out-of-core NRD workflows.
#'
#' @param con A live DuckDB DBI connection.
#' @param memory_limit Optional DuckDB memory limit string (for example,
#'   `"8GB"`). If `NULL`, the pragma is not modified.
#' @param temp_directory Optional existing directory path used by DuckDB for
#'   temporary spill files. If `NULL`, the pragma is not modified.
#' @param threads Optional positive integer for DuckDB worker threads. If
#'   `NULL`, the pragma is not modified.
#' @param preserve_insertion_order Logical scalar controlling DuckDB insertion
#'   order guarantees. Defaults to `FALSE` to reduce buffering pressure for
#'   large parquet scans and materialization.
#'
#' @return The input DuckDB connection, invisibly.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
#'   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#'
#'   nrd_configure_engine(
#'     con,
#'     memory_limit = "8GB",
#'     temp_directory = tempdir(),
#'     threads = 4L,
#'     preserve_insertion_order = FALSE
#'   )
#' }
nrd_configure_engine <- function(
  con,
  memory_limit = NULL,
  temp_directory = NULL,
  threads = NULL,
  preserve_insertion_order = FALSE
) {
  if (!inherits(con, "duckdb_connection") || !DBI::dbIsValid(con)) {
    rlang::abort("`con` must be a valid live DuckDB connection.")
  }

  if (!is.null(memory_limit) &&
    (!is.character(memory_limit) || length(memory_limit) != 1 || is.na(memory_limit) || !nzchar(memory_limit))) {
    rlang::abort("`memory_limit` must be `NULL` or a single non-empty string.")
  }

  if (!is.null(temp_directory)) {
    if (!is.character(temp_directory) || length(temp_directory) != 1 || is.na(temp_directory) || !nzchar(temp_directory)) {
      rlang::abort("`temp_directory` must be `NULL` or a single non-empty path string.")
    }
    if (!dir.exists(temp_directory)) {
      rlang::abort("`temp_directory` must exist when provided.")
    }
  }

  if (!is.null(threads) &&
    (!is.numeric(threads) || length(threads) != 1 || is.na(threads) || threads < 1 || threads != as.integer(threads))) {
    rlang::abort("`threads` must be `NULL` or a single positive integer.")
  }

  if (!is.logical(preserve_insertion_order) || length(preserve_insertion_order) != 1 || is.na(preserve_insertion_order)) {
    rlang::abort("`preserve_insertion_order` must be TRUE or FALSE.")
  }

  if (!is.null(memory_limit)) {
    memory_limit_sql <- as.character(DBI::dbQuoteString(con, memory_limit))
    DBI::dbExecute(con, paste0("PRAGMA memory_limit=", memory_limit_sql))
  }

  if (!is.null(temp_directory)) {
    temp_directory_sql <- as.character(DBI::dbQuoteString(con, temp_directory))
    DBI::dbExecute(con, paste0("PRAGMA temp_directory=", temp_directory_sql))
  }

  if (!is.null(threads)) {
    DBI::dbExecute(con, paste0("PRAGMA threads=", as.integer(threads)))
  }

  DBI::dbExecute(
    con,
    paste0("SET preserve_insertion_order=", tolower(as.character(isTRUE(preserve_insertion_order))))
  )

  invisible(con)
}
