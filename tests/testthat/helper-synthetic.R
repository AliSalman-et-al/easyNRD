# Write a small NRD-shaped parquet fixture so tests exercise real ingest.
make_synthetic_nrd <- function(data = NULL) {
  if (is.null(data)) {
    data <- dplyr::tibble(
      YEAR = c(2019L, 2019L, 2019L, 2020L),
      NRD_VISITLINK = c("A001", "A001", "B001", "A001"),
      KEY_NRD = c(1001L, 1002L, 1003L, 2001L),
      HOSP_NRD = c(10L, 10L, 11L, 12L),
      DISCWT = c(1.0, 1.0, 0.8, 1.2),
      NRD_STRATUM = c(101L, 101L, 102L, 103L),
      NRD_DaysToEvent = c(5L, 20L, 12L, 7L),
      LOS = c(3L, 2L, 4L, 1L),
      DMONTH = c(1L, 2L, 1L, 1L),
      DIED = c(0L, 0L, 0L, 0L),
      FEMALE = c(1L, 1L, 0L, 1L),
      I10_DX1 = c("I2101", "Z001", "J189", "I219"),
      I10_DX2 = c(NA_character_, "I214", NA_character_, "E119"),
      I10_PR1 = c("02703ZZ", "0FT44ZZ", NA_character_, "5A1955Z"),
      I10_PR2 = c(NA_character_, NA_character_, NA_character_, "0BH17EZ")
    )
  }

  path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data, sink = path)
  path
}
