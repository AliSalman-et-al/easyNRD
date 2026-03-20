test_that("nrd_materialize computes a DuckDB checkpoint table", {
  with_duckdb_connection(function(con) {
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
})

test_that("nrd_materialize exports parquet and re-ingests", {
  with_duckdb_connection(function(con) {
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
    on.exit(nrd_close(out), add = TRUE)

    expect_true(file.exists(out_path))
    expect_s3_class(out, "tbl_lazy")
    expect_identical(sort(dplyr::collect(out)$Episode_ID), c(3L, 4L))
  })
})

test_that("nrd_materialize validates parquet path", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(YEAR = 2019L, NRD_VISITLINK = "A005", Episode_ID = 5L)
    DBI::dbWriteTable(con, "materialize_bad_path", dat)
    lazy_tbl <- dplyr::tbl(con, "materialize_bad_path")

    expect_error(
      nrd_materialize(lazy_tbl, path = "bad_path.csv"),
      "must end in `.parquet`",
      fixed = TRUE
    )
  })
})

test_that("nrd_materialize errors on invalid DuckDB connection", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(YEAR = 2019L, NRD_VISITLINK = "A006", Episode_ID = 6L)
    DBI::dbWriteTable(con, "materialize_invalid_con", dat)
    lazy_tbl <- dplyr::tbl(con, "materialize_invalid_con")

    DBI::dbDisconnect(con, shutdown = TRUE)

    expect_error(
      nrd_materialize(lazy_tbl),
      "No active DuckDB connection found"
    )
  })
})

test_that("nrd_materialize preserves owner metadata for in-database checkpoints", {
  parquet_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(
    tibble::tibble(
      YEAR = c(2019L, 2020L),
      NRD_VISITLINK = c("A007", "A008"),
      KEY_NRD = c(7L, 8L),
      Episode_ID = c(7L, 8L)
    ),
    parquet_path
  )

  lazy_tbl <- nrd_ingest(parquet_path)
  owner_before <- nrd_connection_info(lazy_tbl)
  expect_true(owner_before$managed)
  expect_false(owner_before$closed)

  checkpoint <- nrd_materialize(lazy_tbl, name = "m_owner_checkpoint")
  owner_after <- nrd_connection_info(checkpoint)

  expect_true(owner_after$managed)
  expect_false(owner_after$closed)
  expect_identical(owner_before$temp_directory, owner_after$temp_directory)

  expect_no_error(dplyr::collect(checkpoint))
  expect_true(nrd_close(checkpoint))
})
