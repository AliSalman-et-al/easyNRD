# Validate and normalize multi-year pipeline paths while preserving order.
.nrd_normalize_pipeline_paths <- function(paths) {
  if (is.character(paths)) {
    paths_chr <- paths
  } else if (is.list(paths)) {
    if (length(paths) == 0) {
      rlang::abort("`paths` must be a non-empty named character vector or named list of parquet paths.")
    }

    invalid <- !vapply(
      paths,
      function(x) is.character(x) && length(x) == 1 && !is.na(x) && nzchar(x),
      FUN.VALUE = logical(1)
    )

    if (any(invalid)) {
      rlang::abort("`paths` values must each be a single, non-empty parquet path string.")
    }

    paths_chr <- unlist(paths, use.names = FALSE)
    names(paths_chr) <- names(paths)
  } else {
    rlang::abort("`paths` must be a non-empty named character vector or named list of parquet paths.")
  }

  if (length(paths_chr) == 0) {
    rlang::abort("`paths` must be a non-empty named character vector or named list of parquet paths.")
  }

  if (any(!vapply(paths_chr, function(x) !is.na(x) && nzchar(x), FUN.VALUE = logical(1)))) {
    rlang::abort("`paths` values must each be a single, non-empty parquet path string.")
  }

  path_names <- names(paths_chr)
  if (is.null(path_names) || any(is.na(path_names)) || any(!nzchar(path_names))) {
    rlang::abort("`paths` must have non-missing, non-empty names.")
  }

  if (anyDuplicated(path_names)) {
    rlang::abort("`paths` must have unique names.")
  }

  paths_chr
}

# Validate the user-supplied pipeline function.
.nrd_assert_pipeline_fn <- function(.f) {
  if (!is.function(.f)) {
    rlang::abort("`.f` must be a function that takes one ingested NRD table and returns a DuckDB-backed lazy table.")
  }

  invisible(.f)
}

# Validate the optional output directory argument.
.nrd_validate_pipeline_output_dir <- function(output_dir) {
  if (is.null(output_dir)) {
    return(NULL)
  }

  if (!is.character(output_dir) || length(output_dir) != 1 || is.na(output_dir) || !nzchar(output_dir)) {
    rlang::abort("`output_dir` must be NULL or a single, non-empty directory path string.")
  }

  output_dir
}

# Assert that one yearly pipeline result is still lazy and DuckDB-backed.
.nrd_assert_pipeline_result <- function(result) {
  if (!inherits(result, "tbl_lazy")) {
    rlang::abort("`.f` must return a DuckDB-backed lazy table; do not call `collect()` inside `.f`.")
  }

  if (!.nrd_is_duckdb_lazy(result)) {
    rlang::abort("`.f` must return a DuckDB-backed lazy table created from the ingested year data.")
  }

  invisible(result)
}

# Build one deterministic per-year parquet output path.
.nrd_pipeline_output_path <- function(output_dir, year) {
  file.path(output_dir, paste0("NRD_", year, "_pipeline.parquet"))
}

# Process one year through ingest, user pipeline, export, and close.
.nrd_pipeline_one_year <- function(path, year, per_year_path, .f) {
  data <- NULL
  start <- proc.time()[["elapsed"]]

  on.exit(
    {
      if (!is.null(data)) {
        try(nrd_close(data), silent = TRUE)
      }
    },
    add = TRUE
  )

  result <- tryCatch(
    {
      data <- nrd_ingest(path)
      out <- .f(data)
      .nrd_assert_pipeline_result(out)
      nrd_export(out, per_year_path)
      invisible(NULL)
    },
    error = function(err) {
      rlang::abort(
        message = paste0("`nrd_pipeline()` failed for year `", year, "`."),
        parent = err
      )
    }
  )

  elapsed <- proc.time()[["elapsed"]] - start
  message(sprintf("[%s] elapsed=%.3fs", year, elapsed))

  invisible(result)
}

#' Process and pool multi-year NRD analyses safely
#'
#' `nrd_pipeline()` runs a user-supplied single-year lazy pipeline independently
#' for each named NRD parquet path, exports per-year results, then re-ingests the
#' per-year outputs as one pooled DuckDB-backed lazy table.
#'
#' @param paths A named character vector or named list of parquet paths. Names
#'   identify the year labels and are processed in the supplied order. `easyNRD`
#'   supports NRD data years 2016 and later.
#' @param .f A function that takes one ingested single-year DuckDB-backed lazy
#'   table and returns a DuckDB-backed lazy table. The function must not call
#'   `collect()` or [nrd_close()].
#' @param output_dir Optional directory where per-year parquet intermediates are
#'   written as `NRD_<year>_pipeline.parquet`. If `NULL`, `nrd_pipeline()` uses a
#'   temporary intermediate parquet directory.
#'
#' @returns Invisibly returns a pooled DuckDB-backed `tbl_lazy`. The caller must
#'   call [nrd_close()] on the returned object.
#' @export
#'
#' @details
#' `easyNRD` supports NRD data years 2016 and later only. Year 2016 is the first
#' NRD release with a full calendar year of ICD-10-CM/PCS data and the
#' `I10_DX*`/`I10_PR*` column naming convention used throughout the package. The
#' 2015 NRD mixes nine months of ICD-9-CM with three months of ICD-10-CM/PCS and
#' uses a different file structure. Earlier years use ICD-9-CM and different
#' column naming entirely.
#'
#' Each element of `paths` is ingested, processed, exported, and closed in full
#' before the next year begins. Pooling happens only by re-ingesting the
#' exported per-year parquet files after all yearly pipelines complete. This is a
#' row union after yearly processing, not a cross-year join of raw NRD data.
#'
#' This contract makes cross-year readmission linkage structurally impossible.
#' `NRD_VISITLINK`, `HOSP_NRD`, and `NRD_STRATUM` are meaningful only within a
#' single NRD year, so user code in `.f` always sees one year at a time.
#'
#' Do not call [nrd_as_survey()] on the pooled output. NRD survey design is
#' year-specific, and valid weighted analysis requires computing year-specific
#' estimates on single-year outputs and combining those estimates afterward.
#'
#' `nrd_pipeline()` makes no attempt to deduplicate or re-key `KEY_NRD` across a
#' pooled result. `YEAR` remains required to disambiguate records in any pooled
#' table.
#'
#' When `output_dir = NULL`, `nrd_pipeline()` writes per-year outputs to a
#' temporary intermediate parquet directory. That directory is deleted if the
#' pipeline errors, and it is also deleted when [nrd_close()] is called on the
#' pooled object. After [nrd_close()] is called, the pooled table must not be
#' queried further. DuckDB session temp directories are managed separately by the
#' existing [nrd_ingest()] / [nrd_close()] owner mechanism.
#'
#' @examples
#' if (FALSE) {
#'   pooled <- nrd_pipeline(
#'     c(
#'       "2018" = "/path/to/nrd_2018.parquet",
#'       "2019" = "/path/to/nrd_2019.parquet"
#'     ),
#'     function(data) {
#'       data |>
#'         nrd_prepare() |>
#'         dplyr::filter(DIED == 0L)
#'     }
#'   )
#'
#'   nrd_close(pooled)
#' }
nrd_pipeline <- function(paths, .f, output_dir = NULL) {
  paths_chr <- .nrd_normalize_pipeline_paths(paths)
  .nrd_assert_pipeline_fn(.f)
  output_dir <- .nrd_validate_pipeline_output_dir(output_dir)

  pipeline_temp_dir <- NULL
  cleanup_pipeline_temp_dir <- FALSE

  if (is.null(output_dir)) {
    pipeline_temp_dir <- tempfile("easynrd-pipeline-")
    dir.create(pipeline_temp_dir, recursive = TRUE, showWarnings = FALSE)
    output_dir <- pipeline_temp_dir
    cleanup_pipeline_temp_dir <- TRUE
  } else {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  on.exit(
    {
      if (cleanup_pipeline_temp_dir && !is.null(pipeline_temp_dir) && dir.exists(pipeline_temp_dir)) {
        unlink(pipeline_temp_dir, recursive = TRUE, force = TRUE)
      }
    },
    add = TRUE
  )

  per_year_paths <- stats::setNames(
    vapply(names(paths_chr), function(year) .nrd_pipeline_output_path(output_dir, year), FUN.VALUE = character(1)),
    names(paths_chr)
  )

  for (year in names(paths_chr)) {
    .nrd_pipeline_one_year(
      path = paths_chr[[year]],
      year = year,
      per_year_path = per_year_paths[[year]],
      .f = .f
    )
  }

  pooled <- nrd_ingest(unname(per_year_paths))

  if (!is.null(pipeline_temp_dir)) {
    .nrd_add_owner_cleanup_paths(pooled, pipeline_temp_dir)
    cleanup_pipeline_temp_dir <- FALSE
  }

  invisible(pooled)
}
