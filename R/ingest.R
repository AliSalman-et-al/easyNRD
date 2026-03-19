#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   an Arrow query, or an existing DuckDB-backed lazy table.
#'
#' @returns A lazy table, typically `tbl_dbi` backed by DuckDB.
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   raw_nrd <- nrd_ingest(c(
#'     "/path/to/NRD_2019_CORE.parquet",
#'     "/path/to/NRD_2020_CORE.parquet"
#'   ))
#' }
#' }
nrd_ingest <- function(datasets) {
  created_connection <- FALSE
  existing_nrd_env <- NULL
  session_temp_dir <- NULL

  resolve_session_temp_dir <- function(con) {
    from_attr <- attr(con, "nrd_session_temp_dir", exact = TRUE)
    if (is.character(from_attr) && length(from_attr) == 1 && nzchar(from_attr)) {
      return(from_attr)
    }

    tryCatch(
      DBI::dbGetQuery(
        con,
        "SELECT current_setting('temp_directory') AS temp_directory"
      )$temp_directory[[1]],
      error = function(e) NULL
    )
  }

  tbl <- if (inherits(datasets, "tbl_lazy")) {
    .nrd_assert_lazy_duckdb(datasets, arg = "datasets")
    existing_nrd_env <- attr(datasets, "nrd_env", exact = TRUE)
    datasets
  } else if (inherits(datasets, "ArrowTabular") ||
    inherits(datasets, "arrow_dplyr_query")) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    nrd_configure_engine(con, temp_directory = nrd_cache_dir())
    session_temp_dir <- resolve_session_temp_dir(con)
    created_connection <- TRUE
    arrow::to_duckdb(datasets, con = con)
  } else {
    if (!is.character(datasets) || length(datasets) == 0) {
      rlang::abort("`datasets` must be a non-empty character vector of parquet paths.")
    }

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    nrd_configure_engine(con, temp_directory = nrd_cache_dir())
    session_temp_dir <- resolve_session_temp_dir(con)
    created_connection <- TRUE
    arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
      arrow::to_duckdb(con = con)
  }

  .nrd_assert_lazy_duckdb(tbl, arg = "datasets")
  lazy_tbl <- .nrd_standardize_names(tbl)

  if (isTRUE(created_connection)) {
    nrd_env <- new.env(parent = emptyenv())
    nrd_env$con <- con
    nrd_env$session_temp_dir <- session_temp_dir

    reg.finalizer(
      nrd_env,
      function(e) {
        tryCatch(
          {
            if (!is.null(e$con) && DBI::dbIsValid(e$con)) {
              DBI::dbDisconnect(e$con, shutdown = TRUE)
            }
          },
          error = function(err) NULL
        )

        tryCatch(
          {
            if (!is.null(e$session_temp_dir) && dir.exists(e$session_temp_dir)) {
              unlink(e$session_temp_dir, recursive = TRUE, force = TRUE)
            }
          },
          error = function(err) NULL
        )

        invisible(NULL)
      },
      onexit = TRUE
    )

    attr(lazy_tbl, "nrd_env") <- nrd_env
    return(lazy_tbl)
  }

  if (!is.null(existing_nrd_env)) {
    attr(lazy_tbl, "nrd_env") <- existing_nrd_env
  }

  lazy_tbl
}
