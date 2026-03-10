#' Instantiate a Lazy Complex Survey Design for the NRD
#'
#' `nrd_as_survey()` wraps NRD data in an `srvyr` survey object so descriptive
#' epidemiology can stay out-of-core on Arrow and DuckDB backends.
#'
#' @param .data A data frame, lazy table, or a character path accepted by
#'   [nrd_ingest()].
#'
#' @return A `srvyr::tbl_svy` object configured with NRD survey design variables.
#' @details
#' `nrd_as_survey()` supports a dual-track workflow:
#'
#' - Track 1 (out-of-core descriptive epidemiology): compute weighted summaries
#'   directly on the database backend using `dplyr::group_by()` with `srvyr`
#'   verbs like `srvyr::survey_mean()` and `srvyr::survey_total()`.
#' - Track 2 (in-memory inferential modeling): for models such as
#'   `survey::svyglm()` or `survey::svycoxph()`, first call
#'   `dplyr::collect()` on the lazy survey object to bring the targeted subset
#'   into memory.
#'
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   design_lazy <- nrd_ingest("/path/to/linked_output.parquet") |>
#'     nrd_as_survey()
#'
#'   design_lazy |>
#'     dplyr::group_by(outcome_status) |>
#'     dplyr::summarise(readmit_rate = srvyr::survey_mean(IndexEvent == 1L))
#'
#'   model_data <- design_lazy |>
#'     dplyr::filter(IndexEvent == 1L) |>
#'     dplyr::collect()
#'
#'   survey::svyglm(IndexEvent ~ AGE + FEMALE, design = model_data)
#' }
#' }
nrd_as_survey <- function(.data) {
  if (is.character(.data)) {
    .data <- nrd_ingest(.data)
  }

  .nrd_assert_cols(.data, c("HOSP_NRD", "DISCWT", "NRD_STRATUM"))

  srvyr::as_survey(
    .data,
    id = HOSP_NRD,
    weights = DISCWT,
    strata = NRD_STRATUM,
    nest = TRUE
  )
}
