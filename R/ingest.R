#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   an Arrow query, or an existing DuckDB-backed lazy table.
#' @param temp_directory DuckDB spill directory used for temporary on-disk
#'   operations. Defaults to [nrd_cache_dir()], a persistent package cache that
#'   supports larger-than-memory out-of-core processing. `easyNRD` creates
#'   process-specific subdirectories and removes orphaned session caches
#'   automatically. Override this location for HPC nodes or dedicated scratch
#'   storage by setting `EASYNRD_CACHE_DIR` or
#'   `options(easynrd.cache_dir = "/path/to/cache")`.
#' @param memory_limit Optional DuckDB memory limit as a single string, such as
#'   `"8GB"`. When `NULL` (default), DuckDB uses its native memory management.
#' @param threads Optional number of DuckDB worker threads. When `NULL`
#'   (default), DuckDB uses its native thread management.
#' @param preserve_insertion_order Logical flag controlling DuckDB insertion
#'   order guarantees. Defaults to `FALSE`, which reduces buffering pressure
#'   during large parquet processing.
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
#'   ),
#'   temp_directory = "/fast_scratch/duckdb_tmp",
#'   memory_limit = "8GB",
#'   threads = 4L,
#'   preserve_insertion_order = FALSE)
#' }
#' }
nrd_ingest <- function(
  datasets,
  temp_directory = nrd_cache_dir(),
  memory_limit = NULL,
  threads = NULL,
  preserve_insertion_order = FALSE
) {
  if (!is.character(temp_directory) ||
    length(temp_directory) != 1 ||
    is.na(temp_directory) ||
    nchar(temp_directory) == 0) {
    cli::cli_abort(c(
      "`temp_directory` must be a single, non-empty path string.",
      i = "Use a fast local scratch directory when possible."
    ))
  }

  if (!is.null(memory_limit) &&
    (!is.character(memory_limit) || length(memory_limit) != 1 || is.na(memory_limit) || nchar(memory_limit) == 0)) {
    cli::cli_abort(c(
      "`memory_limit` must be `NULL` or a single, non-empty string like `8GB`.",
      i = "Set this explicitly for containerized jobs or institutional HPC quotas."
    ))
  }

  if (!is.null(threads) &&
    (!is.numeric(threads) || length(threads) != 1 || is.na(threads) || threads < 1 || threads != as.integer(threads))) {
    cli::cli_abort(c(
      "`threads` must be `NULL` or a single positive integer.",
      i = "Examples: `threads = 4L`, `threads = 8L`."
    ))
  }

  if (!is.logical(preserve_insertion_order) ||
    length(preserve_insertion_order) != 1 ||
    is.na(preserve_insertion_order)) {
    cli::cli_abort("`preserve_insertion_order` must be TRUE or FALSE.")
  }

  created_connection <- FALSE
  existing_nrd_env <- NULL
  session_temp_dir <- NULL

  tbl <- if (inherits(datasets, "tbl_lazy")) {
    .nrd_assert_lazy_duckdb(datasets, arg = "datasets")
    if (!identical(temp_directory, nrd_cache_dir()) ||
      !is.null(memory_limit) ||
      !is.null(threads) ||
      !identical(preserve_insertion_order, FALSE)) {
      cli::cli_abort(c(
        "Engine options can only be set when creating a new DuckDB connection.",
        i = "For existing lazy tables, call `nrd_configure_engine(dbplyr::remote_con(datasets), ...)` directly."
      ))
    }
    existing_nrd_env <- attr(datasets, "nrd_env", exact = TRUE)
    datasets
  } else if (inherits(datasets, "ArrowTabular") ||
    inherits(datasets, "arrow_dplyr_query")) {
    if (!dir.exists(temp_directory)) {
      dir.create(temp_directory, recursive = TRUE, showWarnings = FALSE)
    }
    .nrd_cleanup_session_temp_dirs(temp_directory = temp_directory, force = FALSE)
    session_temp_dir <- file.path(temp_directory, paste0("easyNRD_", Sys.getpid()))
    dir.create(session_temp_dir, showWarnings = FALSE, recursive = TRUE)

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    nrd_configure_engine(
      con,
      memory_limit = memory_limit,
      temp_directory = session_temp_dir,
      threads = threads,
      preserve_insertion_order = preserve_insertion_order
    )
    created_connection <- TRUE
    arrow::to_duckdb(datasets, con = con)
  } else {
    if (!is.character(datasets) || length(datasets) == 0) {
      rlang::abort("`datasets` must be a non-empty character vector of parquet paths.")
    }

    if (!dir.exists(temp_directory)) {
      dir.create(temp_directory, recursive = TRUE, showWarnings = FALSE)
    }
    .nrd_cleanup_session_temp_dirs(temp_directory = temp_directory, force = FALSE)
    session_temp_dir <- file.path(temp_directory, paste0("easyNRD_", Sys.getpid()))
    dir.create(session_temp_dir, showWarnings = FALSE, recursive = TRUE)

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    nrd_configure_engine(
      con,
      memory_limit = memory_limit,
      temp_directory = session_temp_dir,
      threads = threads,
      preserve_insertion_order = preserve_insertion_order
    )
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
