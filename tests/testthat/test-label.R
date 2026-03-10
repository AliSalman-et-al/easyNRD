test_that("nrd_label default labels known coded columns", {
  mock_data <- tibble::tibble(
    FEMALE = c(0L, 1L, 1L),
    PAY1 = c(1L, 3L, 6L),
    AGE = c(65L, 70L, 58L)
  )

  out <- nrd_label(mock_data)

  expect_type(out$FEMALE, "character")
  expect_type(out$PAY1, "character")
  expect_identical(out$FEMALE, c("Male", "Female", "Female"))
  expect_identical(out$PAY1, c("Medicare", "Private", "Other"))
  expect_type(out$AGE, "integer")
  expect_identical(out$AGE, mock_data$AGE)
})

test_that("nrd_label explicit selection updates only selected columns", {
  mock_data <- tibble::tibble(
    FEMALE = c(0L, 1L),
    PAY1 = c(2L, 4L),
    AGE = c(62L, 74L)
  )

  out <- nrd_label(mock_data, FEMALE)

  expect_type(out$FEMALE, "character")
  expect_identical(out$FEMALE, c("Male", "Female"))
  expect_type(out$PAY1, "integer")
  expect_identical(out$PAY1, mock_data$PAY1)
})

test_that("nrd_label keep_original appends label columns", {
  mock_data <- tibble::tibble(
    FEMALE = c(0L, 1L),
    PAY1 = c(1L, 5L),
    AGE = c(61L, 77L)
  )

  out <- nrd_label(mock_data, FEMALE, .keep_original = TRUE)

  expect_type(out$FEMALE, "integer")
  expect_true("FEMALE_label" %in% names(out))
  expect_type(out$FEMALE_label, "character")
  expect_identical(out$FEMALE_label, c("Male", "Female"))
})

test_that("nrd_label translates on lazy duckdb backend", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(
    con,
    "label_mock",
    tibble::tibble(
      FEMALE = c(0L, 1L),
      PAY1 = c(1L, 3L),
      AGE = c(60L, 70L)
    )
  )

  lazy_tbl <- dplyr::tbl(con, "label_mock")

  labeled <- nrd_label(lazy_tbl, FEMALE)

  expect_no_error(dbplyr::sql_render(labeled))
  expect_match(dbplyr::sql_render(labeled), "CASE", ignore.case = TRUE)
})

test_that("nrd_label handles float coded values on lazy duckdb backend", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(
    con,
    "label_mock_float",
    tibble::tibble(
      FEMALE = c(0.0, 1.0, 9.0),
      PAY1 = c(1.0, 3.0, 8.0),
      AGE = c(60, 70, 80)
    )
  )

  lazy_tbl <- dplyr::tbl(con, "label_mock_float")

  labeled <- nrd_label(lazy_tbl, FEMALE, PAY1)

  expect_no_error(dbplyr::sql_render(labeled))

  collected <- dplyr::collect(labeled)

  expect_identical(collected$FEMALE, c("Male", "Female", "9"))
  expect_identical(collected$PAY1, c("Medicare", "Private", "8"))
})
