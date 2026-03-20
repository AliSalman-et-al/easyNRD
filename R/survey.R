#' Wrap NRD data in a survey design
#'
#' `nrd_as_survey()` creates an `srvyr` survey object using the NRD design
#' variables and a deliberate eager pass to compute degrees of freedom.
#'
#' @param data An in-memory data frame or DuckDB-backed lazy table.
#'
#' @returns A `srvyr::tbl_svy` object.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/nrd.parquet")
#'   design <- nrd_as_survey(data)
#' }
nrd_as_survey <- function(data) {
  .nrd_assert_cols(data, c("HOSP_NRD", "DISCWT", "NRD_STRATUM"))

  # This is the one deliberate eager pass needed to compute survey degrees of
  # freedom from the full design before any domain subsetting.
  degf <- data |>
    dplyr::summarise(
      .nrd_cluster_n = dplyr::n_distinct(HOSP_NRD),
      .nrd_strata_n = dplyr::n_distinct(NRD_STRATUM)
    ) |>
    dplyr::mutate(.nrd_degf = as.integer(.nrd_cluster_n - .nrd_strata_n)) |>
    dplyr::pull(.nrd_degf)

  srvyr::as_survey(
    data,
    id = HOSP_NRD,
    weights = DISCWT,
    strata = NRD_STRATUM,
    nest = TRUE,
    degf = degf
  )
}
