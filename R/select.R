#' Select Analytical Variables with Protected NRD Design Columns
#'
#' `nrd_select()` applies projection pushdown early in the NRD pipeline so only
#' required analytical variables are carried forward to DuckDB materialization.
#' This reduces memory pressure and disk I/O for wide NRD extracts.
#'
#' The function always retains core architecture variables needed for episode
#' linkage and complex survey design, even if they are not explicitly requested:
#' `YEAR`, `NRD_VISITLINK`, `Episode_ID`, `HOSP_NRD`, `DISCWT`, and
#' `NRD_STRATUM`.
#'
#' If readmission linkage outputs are present in `.data`, they are also retained
#' automatically: `IndexEvent`, `time_to_event`, and `outcome_status`.
#'
#' @param .data A data frame, lazy table, or Arrow query.
#' @param ... <[`tidy-select`][dplyr::dplyr_tidy_select]> Columns to keep.
#'   Supports unquoted column names and selection helpers, including character
#'   vectors returned by `nrd_demographics()` and `nrd_hospital_vars()`.
#'
#' @return A subsetted object of the same backend class as `.data`.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_select(is_ami, nrd_demographics(), nrd_hospital_vars()) |>
#'     nrd_export("/path/to/analysis_subset.parquet")
#' }
nrd_select <- function(.data, ...) {
  mandatory_vars <- c(
    "YEAR",
    "NRD_VISITLINK",
    "Episode_ID",
    "HOSP_NRD",
    "DISCWT",
    "NRD_STRATUM"
  )

  conditional_vars <- c("IndexEvent", "time_to_event", "outcome_status")

  dplyr::select(
    .data,
    dplyr::all_of(mandatory_vars),
    dplyr::any_of(conditional_vars),
    ...
  )
}

#' Standard NRD Demographic Variable Names
#'
#' Returns the standard NRD demographic variables as a character vector.
#' Use inside `nrd_select()`, `dplyr::select()`, or with
#' `tidyselect::any_of()` when some variables may be absent.
#'
#' @return A character vector of demographic variable names.
#' @export
#'
#' @examples
#' nrd_demographics()
nrd_demographics <- function() {
  c("AGE", "FEMALE", "ZIPINC_QRTL", "PAY1", "RESIDENT", "PL_NCHS")
}

#' Standard NRD Hospital Structural Variable Names
#'
#' Returns the standard NRD hospital structural variables as a character vector.
#' Use inside `nrd_select()`, `dplyr::select()`, or with
#' `tidyselect::any_of()` when some variables may be absent.
#'
#' @return A character vector of hospital structural variable names.
#' @export
#'
#' @examples
#' nrd_hospital_vars()
nrd_hospital_vars <- function() {
  c("HOSP_BEDSIZE", "H_CONTRL", "HOSP_URCAT4", "HOSP_UR_TEACH")
}
