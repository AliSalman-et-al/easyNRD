test_that("nrd_materialize computes a DuckDB checkpoint table", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A002"),
    Episode_ID = c(1L, 2L)
  )

  DBI::dbWriteTable(con, "materialize_input", dat)
  lazy_tbl <- dplyr::tbl(con, "materialize_input")

  out <- nrd_materialize(lazy_tbl, name = "m_test")

  expect_s3_class(out, "tbl_lazy")
  expect_identical(sort(dplyr::collect(out)$Episode_ID), c(1L, 2L))
})

test_that("nrd_materialize exports parquet and re-ingests", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    YEAR = c(2019L, 2020L),
    NRD_VISITLINK = c("A003", "A004"),
    Episode_ID = c(3L, 4L)
  )

  DBI::dbWriteTable(con, "materialize_export_input", dat)
  lazy_tbl <- dplyr::tbl(con, "materialize_export_input")
  out_path <- tempfile(fileext = ".parquet")

  out <- nrd_materialize(
    lazy_tbl,
    path = out_path,
    row_group_size = 1000L
  )

  expect_true(file.exists(out_path))
  expect_s3_class(out, "tbl_lazy")
  expect_identical(sort(dplyr::collect(out)$Episode_ID), c(3L, 4L))
})

test_that("nrd_materialize validates parquet path", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(YEAR = 2019L, NRD_VISITLINK = "A005", Episode_ID = 5L)
  DBI::dbWriteTable(con, "materialize_bad_path", dat)
  lazy_tbl <- dplyr::tbl(con, "materialize_bad_path")

  expect_error(
    nrd_materialize(lazy_tbl, path = "bad_path.csv"),
    "must end in `.parquet`",
    fixed = TRUE
  )
})

test_that("nrd_materialize errors on invalid DuckDB connection", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  dat <- tibble::tibble(YEAR = 2019L, NRD_VISITLINK = "A006", Episode_ID = 6L)
  DBI::dbWriteTable(con, "materialize_invalid_con", dat)
  lazy_tbl <- dplyr::tbl(con, "materialize_invalid_con")

  DBI::dbDisconnect(con, shutdown = TRUE)

  expect_error(
    nrd_materialize(lazy_tbl),
    "No active DuckDB connection found"
  )
})
