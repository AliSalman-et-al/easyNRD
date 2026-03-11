#' Ingest NRD Parquet Files Lazily
#'
#' `nrd_ingest()` initializes a lazy NRD table from one or more parquet files.
#' The function standardizes common HCUP name variants and keeps data out-of-core
#' for downstream pipeline steps.
#'
#' @param datasets Character vector of parquet file paths, an Arrow dataset,
#'   an Arrow query, or an existing DuckDB-backed lazy table.
#' @param temp_directory DuckDB spill directory used for temporary on-disk
#'   operations. Defaults to [base::tempdir()]. Provide a single path string
#'   (for example, a fast local SSD directory) to reduce RAM pressure during
#'   large window operations.
#' @param memory_limit Optional DuckDB memory limit as a single string, such as
#'   `"8GB"`. When `NULL` (default), `easyNRD` sets DuckDB memory usage to 80%
#'   of available system RAM.
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
nrd_ingest <- function(datasets, temp_directory = tempdir(), memory_limit = NULL) {
  if (!is.character(temp_directory) ||
    length(temp_directory) != 1 ||
    is.na(temp_directory) ||
    nchar(temp_directory) == 0) {
    rlang::abort("`temp_directory` must be a single, non-empty string.")
  }

  if (!is.null(memory_limit) &&
    (!is.character(memory_limit) || length(memory_limit) != 1 || is.na(memory_limit) || nchar(memory_limit) == 0)) {
    rlang::abort("`memory_limit` must be NULL or a single, non-empty string like '8GB'.")
  }

  detect_available_ram_bytes <- function() {
    if (.Platform$OS.type == "windows") {
      available_mb <- suppressWarnings(utils::memory.limit())
      if (is.finite(available_mb) && available_mb > 0) {
        return(as.numeric(available_mb) * 1024^2)
      }
      return(NA_real_)
    }

    if (Sys.info()[["sysname"]] == "Linux") {
      if (file.exists("/proc/meminfo")) {
        mem_lines <- readLines("/proc/meminfo", warn = FALSE)
        mem_available <- grep("^MemAvailable:", mem_lines, value = TRUE)
        mem_total <- grep("^MemTotal:", mem_lines, value = TRUE)
        mem_line <- if (length(mem_available) > 0) mem_available[[1]] else if (length(mem_total) > 0) mem_total[[1]] else NA_character_
        if (!is.na(mem_line)) {
          kb <- suppressWarnings(as.numeric(gsub("[^0-9]", "", mem_line)))
          if (is.finite(kb) && kb > 0) {
            return(kb * 1024)
          }
        }
      }
      return(NA_real_)
    }

    if (Sys.info()[["sysname"]] == "Darwin") {
      memsize <- suppressWarnings(system2("sysctl", c("-n", "hw.memsize"), stdout = TRUE, stderr = FALSE))
      if (length(memsize) > 0) {
        bytes <- suppressWarnings(as.numeric(memsize[[1]]))
        if (is.finite(bytes) && bytes > 0) {
          return(bytes)
        }
      }
      return(NA_real_)
    }

    NA_real_
  }

  auto_memory_limit <- NULL
  if (is.null(memory_limit)) {
    available_ram <- detect_available_ram_bytes()
    if (is.finite(available_ram) && available_ram > 0) {
      memory_mb <- floor((available_ram * 0.8) / 1024^2)
      if (is.finite(memory_mb) && memory_mb > 0) {
        auto_memory_limit <- paste0(memory_mb, "MB")
      }
    }
  }

  configure_duckdb <- function(con) {
    if (!dir.exists(temp_directory)) {
      dir.create(temp_directory, recursive = TRUE, showWarnings = FALSE)
    }

    temp_directory_sql <- as.character(DBI::dbQuoteString(con, temp_directory))
    DBI::dbExecute(con, paste0("PRAGMA temp_directory=", temp_directory_sql))

    effective_memory_limit <- if (is.null(memory_limit)) auto_memory_limit else memory_limit
    if (!is.null(effective_memory_limit)) {
      memory_limit_sql <- as.character(DBI::dbQuoteString(con, effective_memory_limit))
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
