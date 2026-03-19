#' Configure DuckDB Engine Pragmas for easyNRD Pipelines
#'
#' `nrd_configure_engine()` applies DuckDB runtime pragmas to an active
#' connection. Use this to configure memory use, temporary spill location, and
#' worker thread allocation before running large out-of-core NRD workflows.
#'
#' @param con A live DuckDB DBI connection.
#' @param memory_limit Optional DuckDB memory limit string (for example,
#'   `"8GB"`). If `NULL`, the pragma is not modified.
#' @param temp_directory Optional existing base directory path used by DuckDB
#'   for temporary spill files. If non-`NULL`, a process-specific
#'   `easyNRD_<pid>` subdirectory is created and configured as DuckDB's
#'   temporary directory.
#' @param threads Optional positive integer for DuckDB worker threads. If
#'   `NULL`, the pragma is not modified.
#' @param preserve_insertion_order Optional logical scalar controlling DuckDB
#'   insertion order guarantees. If `NULL`, this setting is not modified.
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
  preserve_insertion_order = NULL
) {
  if (!inherits(con, "duckdb_connection") || !DBI::dbIsValid(con)) {
    rlang::abort("`con` must be a valid live DuckDB connection.")
  }

  if (!is.null(memory_limit) &&
    (!is.character(memory_limit) || length(memory_limit) != 1 || is.na(memory_limit) || !nzchar(memory_limit))) {
    rlang::abort("`memory_limit` must be `NULL` or a single non-empty string.")
  }

  if (!is.null(temp_directory) &&
    (!is.character(temp_directory) || length(temp_directory) != 1 || is.na(temp_directory) || !nzchar(temp_directory))) {
    rlang::abort("`temp_directory` must be `NULL` or a single non-empty path string.")
  }

  if (!is.null(threads) &&
    (!is.numeric(threads) || length(threads) != 1 || is.na(threads) || threads < 1 || threads != as.integer(threads))) {
    rlang::abort("`threads` must be `NULL` or a single positive integer.")
  }

  if (!is.null(preserve_insertion_order) &&
    (!is.logical(preserve_insertion_order) || length(preserve_insertion_order) != 1 || is.na(preserve_insertion_order))) {
    rlang::abort("`preserve_insertion_order` must be `NULL`, TRUE, or FALSE.")
  }

  session_temp_dir <- NULL
  if (!is.null(temp_directory)) {
    if (!dir.exists(temp_directory)) {
      rlang::abort("`temp_directory` must exist when provided.")
    }

    .nrd_cleanup_session_temp_dirs(temp_directory = temp_directory, force = FALSE)

    session_temp_dir <- file.path(temp_directory, paste0("easyNRD_", Sys.getpid()))
    dir.create(session_temp_dir, showWarnings = FALSE, recursive = TRUE)
  }

  if (!is.null(memory_limit)) {
    memory_limit_sql <- as.character(DBI::dbQuoteString(con, memory_limit))
    DBI::dbExecute(con, paste0("PRAGMA memory_limit=", memory_limit_sql))
  }

  if (!is.null(temp_directory)) {
    temp_directory_sql <- as.character(DBI::dbQuoteString(con, session_temp_dir))
    DBI::dbExecute(con, paste0("PRAGMA temp_directory=", temp_directory_sql))
  }

  if (!is.null(threads)) {
    DBI::dbExecute(con, paste0("PRAGMA threads=", as.integer(threads)))
  }

  if (!is.null(preserve_insertion_order)) {
    DBI::dbExecute(
      con,
      paste0("SET preserve_insertion_order=", tolower(as.character(isTRUE(preserve_insertion_order))))
    )
  }

  attr(con, "nrd_session_temp_dir") <- session_temp_dir

  invisible(con)
}
