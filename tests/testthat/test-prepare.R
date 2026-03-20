test_that("Discharge_Day equals NRD_DAYSTOEVENT plus LOS for known values", {
  path <- make_synthetic_nrd(tibble::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1001L, 1002L),
    NRD_DaysToEvent = c(5L, 10L),
    LOS = c(3L, 4L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L)
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- data |>
    nrd_prepare() |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$Discharge_Day, c(8L, 14L))
})

test_that("missing required columns produce a clear error naming the missing column", {
  path <- make_synthetic_nrd(tibble::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 1001L,
    NRD_DaysToEvent = 5L,
    LOS = 3L,
    DMONTH = 1L
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_error(
    nrd_prepare(data),
    "DIED"
  )
})

test_that("row count is unchanged after nrd_prepare", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  before <- data |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(n)

  after <- data |>
    nrd_prepare() |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(n)

  expect_identical(after, before)
})

test_that("nrd_prepare does not modify existing columns", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  before <- data |>
    dplyr::select(YEAR, NRD_VISITLINK, KEY_NRD, NRD_DAYSTOEVENT, LOS, DMONTH, DIED) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  after <- data |>
    nrd_prepare() |>
    dplyr::select(YEAR, NRD_VISITLINK, KEY_NRD, NRD_DAYSTOEVENT, LOS, DMONTH, DIED) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(after, before)
})

test_that("nrd_prepare returns a lazy DuckDB table and does not collect", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_prepare(data)

  expect_s3_class(out, "tbl_lazy")
  expect_true(inherits(dbplyr::remote_con(out), "duckdb_connection"))
  expect_false(inherits(out, c("data.frame", "tbl_df")))
})

test_that("Discharge_Day is NA when LOS is NA", {
  path <- make_synthetic_nrd(tibble::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1001L, 1002L),
    NRD_DaysToEvent = c(5L, 10L),
    LOS = c(NA_integer_, 4L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L)
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- data |>
    nrd_prepare() |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_true(is.na(out$Discharge_Day[[1]]))
  expect_identical(out$Discharge_Day[[2]], 14L)
})
