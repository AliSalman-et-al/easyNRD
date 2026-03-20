#' Select analytical variables while preserving core NRD identifiers
#'
#' `nrd_select()` keeps linkage-critical identifiers, retains common analysis
#' columns when present, and appends user-selected variables.
#'
#' @param data A lazy table or in-memory data frame.
#' @param ... <[`tidy-select`][dplyr::dplyr_tidy_select]> Columns to append.
#'
#' @returns An object of the same backend class with selected columns.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/nrd.parquet")
#'   nrd_select(data, nrd_demographics(), nrd_hospital_vars())
#' }
nrd_select <- function(data, ...) {
  .nrd_assert_cols(data, c("YEAR", "NRD_VISITLINK", "KEY_NRD"))

  retained_core <- c("YEAR", "NRD_VISITLINK", "KEY_NRD")
  retained_optional <- c(
    "HOSP_NRD", "DISCWT", "NRD_STRATUM", "IndexEvent", "time_to_event",
    "outcome_status", "Discharge_Day", "DMONTH", "DIED",
    "NRD_DAYSTOEVENT", "LOS"
  )

  dplyr::select(
    data,
    dplyr::all_of(retained_core),
    dplyr::any_of(retained_optional),
    ...
  )
}

#' Return standard NRD demographic variables
#'
#' @returns A character vector of demographic variable names.
#' @export
#'
#' @examples
#' nrd_demographics()
nrd_demographics <- function() {
  c("AGE", "FEMALE", "ZIPINC_QRTL", "PAY1", "RESIDENT", "PL_NCHS")
}

#' Return standard NRD hospital structural variables
#'
#' @returns A character vector of hospital variable names.
#' @export
#'
#' @examples
#' nrd_hospital_vars()
nrd_hospital_vars <- function() {
  c("HOSP_BEDSIZE", "H_CONTRL", "HOSP_URCAT4", "HOSP_UR_TEACH")
}
