#' Prepare an NRD discharge table for linkage
#'
#' `nrd_prepare()` adds `Discharge_Day` to a DuckDB-backed NRD table without
#' changing its denominator or collecting data into memory.
#'
#' @param data A DuckDB-backed lazy table returned by [nrd_ingest()].
#'
#' @returns A DuckDB-backed `tbl_lazy` with `Discharge_Day` added.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/nrd.parquet")
#'   prepared <- nrd_prepare(data)
#' }
nrd_prepare <- function(data) {
  .nrd_assert_duckdb_lazy(data)
  .nrd_assert_cols(
    data,
    c("NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT", "LOS", "DMONTH", "DIED", "YEAR")
  )

  dplyr::mutate(
    data,
    Discharge_Day = NRD_DAYSTOEVENT + LOS
  )
}
