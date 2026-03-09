#' Convert Materialized NRD Data to a Survey Design
#'
#' `nrd_as_survey()` reads a materialized parquet dataset (or accepts an
#' in-memory data frame) and returns an HCUP-ready complex survey design object.
#'
#' @param dataset File path to a parquet dataset produced by `nrd_export()`, or
#'   a data frame/tibble already in memory.
#'
#' @return A `survey::svydesign` object with NRD design variables.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   des <- nrd_as_survey("/path/to/linked_output.parquet")
#' }
#' }
nrd_as_survey <- function(dataset) {
  dat <- if (is.character(dataset) && length(dataset) == 1) {
    as.data.frame(arrow::read_parquet(dataset))
  } else if (is.data.frame(dataset)) {
    dataset
  } else {
    rlang::abort("`dataset` must be a parquet file path or an in-memory data frame.")
  }

  .nrd_assert_cols(dat, c("HOSP_NRD", "DISCWT", "NRD_STRATUM"))

  survey::svydesign(
    ids = ~HOSP_NRD,
    weights = ~DISCWT,
    strata = ~NRD_STRATUM,
    data = dat,
    nest = TRUE
  )
}
