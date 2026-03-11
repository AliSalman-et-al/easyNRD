test_that("nrd_link_readmissions de-indexes incomplete follow-up by default", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001"),
    Episode_ID = c(1L, 2L),
    Episode_KEY_NRD = c(1001L, 1002L),
    Episode_Admission_Day = c(345, 355),
    Episode_Discharge_Day = c(350, 360),
    Episode_DMONTH = c(12L, 12L),
    DIED = c(0L, 0L)
  )

  DBI::dbWriteTable(con, "episode_input", dat)
  lazy_tbl <- dplyr::tbl(con, "episode_input")

  out <- lazy_tbl |>
    nrd_link_readmissions(
      index_condition = Episode_ID == 1L,
      readmit_condition = DIED == 0L,
      window = 30L
    ) |>
    dplyr::collect()

  index_row <- dplyr::filter(out, Episode_ID == 1L)
  expect_equal(index_row$IndexEvent, 0L)
  expect_true(is.na(index_row$time_to_event))
  expect_true(is.na(index_row$outcome_status))
})

test_that("nrd_link_readmissions supports censored follow-up retention", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A002", "A002"),
    Episode_ID = c(1L, 2L),
    Episode_KEY_NRD = c(2001L, 2002L),
    Episode_Admission_Day = c(345, 355),
    Episode_Discharge_Day = c(350, 360),
    Episode_DMONTH = c(12L, 12L),
    DIED = c(0L, 0L)
  )

  DBI::dbWriteTable(con, "episode_input_censored", dat)
  lazy_tbl <- dplyr::tbl(con, "episode_input_censored")

  out <- lazy_tbl |>
    nrd_link_readmissions(
      index_condition = Episode_ID == 1L,
      readmit_condition = DIED == 0L,
      window = 30L,
      allow_censored_followup = TRUE
    ) |>
    dplyr::collect()

  index_row <- dplyr::filter(out, Episode_ID == 1L)
  expect_equal(index_row$IndexEvent, 1L)
  expect_equal(index_row$time_to_event, 5)
  expect_identical(index_row$outcome_status, "Readmitted")
})
