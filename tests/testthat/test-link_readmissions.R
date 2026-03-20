test_that("HCUP-style index definition excludes deaths and same-day/transfer combined stays", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2019L, 2019L, 2019L),
      NRD_VISITLINK = c("A001", "A002", "A003", "A003"),
      Episode_ID = c(1L, 1L, 1L, 2L),
      Episode_KEY_NRD = c(1001L, 2001L, 3001L, 3002L),
      Episode_Admission_Day = c(100, 120, 140, 150),
      Episode_Discharge_Day = c(105, 124, 145, 152),
      Episode_DMONTH = c(4L, 4L, 5L, 5L),
      Episode_SAMEDAYEVENT = c(0L, 0L, 1L, 0L),
      DIED = c(1L, 0L, 0L, 0L)
    )

    DBI::dbWriteTable(con, "episode_input_hcup_index", dat)

    out <- dplyr::tbl(con, "episode_input_hcup_index") |>
      nrd_link_readmissions(
        index_condition = DIED == 0L & Episode_SAMEDAYEVENT == 0L,
        readmit_condition = TRUE,
        window = 30L
      ) |>
      dplyr::arrange(NRD_VISITLINK, Episode_ID) |>
      dplyr::collect()

    expect_equal(out$IndexEvent[out$Episode_KEY_NRD == 1001L], 0L)
    expect_equal(out$IndexEvent[out$Episode_KEY_NRD == 3001L], 0L)
    expect_equal(out$IndexEvent[out$Episode_KEY_NRD == 3002L], 1L)
  })
})

test_that("all-cause readmission counts even when readmission discharge has death", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2019L),
      NRD_VISITLINK = c("A010", "A010"),
      Episode_ID = c(1L, 2L),
      Episode_KEY_NRD = c(1010L, 1011L),
      Episode_Admission_Day = c(10, 15),
      Episode_Discharge_Day = c(12, 18),
      Episode_DMONTH = c(1L, 1L),
      Episode_SAMEDAYEVENT = c(0L, 0L),
      DIED = c(0L, 1L)
    )

    DBI::dbWriteTable(con, "episode_input_death_readmit", dat)

    out <- dplyr::tbl(con, "episode_input_death_readmit") |>
      nrd_link_readmissions(
        index_condition = DIED == 0L & Episode_SAMEDAYEVENT == 0L,
        readmit_condition = TRUE,
        window = 30L,
        readmit_vars = c(DIED, Episode_KEY_NRD)
      ) |>
      dplyr::collect()

    index_row <- out[out$Episode_ID == 1L, ]
    expect_equal(index_row$IndexEvent, 1L)
    expect_identical(index_row$outcome_status, "Readmitted")
    expect_equal(index_row$time_to_event, 3)
    expect_equal(index_row$readmit_DIED, 1L)
    expect_equal(index_row$readmit_Episode_KEY_NRD, 1011L)
  })
})

test_that("readmissions can become later index events and only first qualifying readmission is linked", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2019L, 2019L, 2019L),
      NRD_VISITLINK = c("A020", "A020", "A020", "A020"),
      Episode_ID = c(1L, 2L, 3L, 4L),
      Episode_KEY_NRD = c(2010L, 2011L, 2012L, 2013L),
      Episode_Admission_Day = c(10, 15, 20, 60),
      Episode_Discharge_Day = c(10, 16, 21, 61),
      Episode_DMONTH = c(1L, 1L, 1L, 3L),
      Episode_SAMEDAYEVENT = c(0L, 0L, 0L, 0L),
      DIED = c(0L, 0L, 0L, 0L)
    )

    DBI::dbWriteTable(con, "episode_input_recurrent_index", dat)

    out <- dplyr::tbl(con, "episode_input_recurrent_index") |>
      nrd_link_readmissions(
        index_condition = DIED == 0L & Episode_SAMEDAYEVENT == 0L,
        readmit_condition = TRUE,
        window = 30L,
        readmit_vars = c(Episode_KEY_NRD)
      ) |>
      dplyr::arrange(Episode_ID) |>
      dplyr::collect()

    row1 <- out[out$Episode_ID == 1L, ]
    row2 <- out[out$Episode_ID == 2L, ]

    expect_equal(row1$IndexEvent, 1L)
    expect_equal(row1$time_to_event, 5)
    expect_identical(row1$outcome_status, "Readmitted")
    expect_equal(row1$readmit_Episode_KEY_NRD, 2011L)

    expect_equal(row2$IndexEvent, 1L)
    expect_equal(row2$time_to_event, 4)
    expect_identical(row2$outcome_status, "Readmitted")
    expect_equal(row2$readmit_Episode_KEY_NRD, 2012L)
  })
})

test_that("linkage is year-bounded even with identical NRD_VISITLINK", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2020L),
      NRD_VISITLINK = c("A030", "A030"),
      Episode_ID = c(1L, 1L),
      Episode_KEY_NRD = c(3010L, 4010L),
      Episode_Admission_Day = c(348, 5),
      Episode_Discharge_Day = c(350, 7),
      Episode_DMONTH = c(12L, 1L),
      Episode_SAMEDAYEVENT = c(0L, 0L),
      DIED = c(0L, 0L)
    )

    DBI::dbWriteTable(con, "episode_input_cross_year", dat)

    out <- dplyr::tbl(con, "episode_input_cross_year") |>
      nrd_link_readmissions(
        index_condition = DIED == 0L & Episode_SAMEDAYEVENT == 0L,
        readmit_condition = TRUE,
        window = 30L,
        readmit_vars = c(Episode_KEY_NRD),
        allow_censored_followup = TRUE
      ) |>
      dplyr::collect()

    dec_row <- out[out$YEAR == 2019L, ]
    expect_equal(dec_row$IndexEvent, 1L)
    expect_identical(dec_row$outcome_status, "Censored")
    expect_true(is.na(dec_row$readmit_Episode_KEY_NRD))
  })
})

test_that("index eligibility uses discharge day for year-end follow-up completeness", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2019L),
      NRD_VISITLINK = c("A040", "A041"),
      Episode_ID = c(1L, 1L),
      Episode_KEY_NRD = c(5010L, 5011L),
      Episode_Admission_Day = c(330, 334),
      Episode_Discharge_Day = c(335, 336),
      Episode_DMONTH = c(11L, 11L),
      Episode_SAMEDAYEVENT = c(0L, 0L),
      DIED = c(0L, 0L)
    )

    DBI::dbWriteTable(con, "episode_input_day_boundary", dat)

    out <- dplyr::tbl(con, "episode_input_day_boundary") |>
      nrd_link_readmissions(
        index_condition = TRUE,
        readmit_condition = TRUE,
        window = 30L,
        allow_censored_followup = FALSE
      ) |>
      dplyr::arrange(Episode_KEY_NRD) |>
      dplyr::collect()

    expect_equal(out$IndexEvent[out$Episode_KEY_NRD == 5010L], 1L)
    expect_equal(out$IndexEvent[out$Episode_KEY_NRD == 5011L], 0L)
  })
})
