test_that("nrd_build_episodes collapses contiguous transfer-linked stays", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2019L, 2019L, 2019L, 2019L),
      NRD_VISITLINK = c("A001", "A001", "A001", "A002"),
      KEY_NRD = c(1001L, 1002L, 1003L, 2001L),
      NRD_DAYSTOEVENT = c(10L, 13L, 30L, 50L),
      LOS = c(3L, 2L, 1L, 4L),
      DISPUNIFORM = c(2L, 1L, 1L, 1L),
      DMONTH = c(1L, 1L, 1L, 2L),
      DIED = c(0L, 0L, 0L, 0L),
      I10_DX1 = c("I214", "I500", "J189", "I214"),
      TOTCHG = c(1000, 2000, 500, 3000),
      SAMEDAYEVENT = c(0L, 1L, 0L, 0L)
    )

    DBI::dbWriteTable(con, "raw_nrd", dat)

    out <- dplyr::tbl(con, "raw_nrd") |>
      nrd_build_episodes() |>
      dplyr::arrange(NRD_VISITLINK, Episode_ID) |>
      dplyr::collect()

    a001_ep1 <- out[out$NRD_VISITLINK == "A001" & out$Episode_ID == 1L, ]
    a001_ep2 <- out[out$NRD_VISITLINK == "A001" & out$Episode_ID == 2L, ]

    expect_equal(nrow(a001_ep1), 1)
    expect_equal(a001_ep1$Episode_N_Stays, 2L)
    expect_equal(a001_ep1$Episode_Admission_Day, 10)
    expect_equal(a001_ep1$Episode_Discharge_Day, 15)
    expect_equal(a001_ep1$Episode_LOS, 5)
    expect_equal(a001_ep1$Episode_KEY_NRD, 1002L)
    expect_equal(a001_ep1$Episode_Index_KEY_NRD, 1001L)
    expect_equal(a001_ep1$Episode_DX10_Principal, "I214")
    expect_equal(a001_ep1$Episode_TOTCHG, 3000)
    expect_equal(a001_ep1$Episode_SAMEDAYEVENT, 1L)

    expect_equal(nrow(a001_ep2), 1)
    expect_equal(a001_ep2$Episode_N_Stays, 1L)
    expect_equal(a001_ep2$Episode_KEY_NRD, 1003L)
    expect_equal(a001_ep2$Episode_DMONTH, 1L)
  })
})

test_that("nrd_build_episodes retains NA SAMEDAYEVENT when source column absent", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = c(2020L, 2020L),
      NRD_VISITLINK = c("B001", "B001"),
      KEY_NRD = c(3001L, 3002L),
      NRD_DAYSTOEVENT = c(100L, 103L),
      LOS = c(3L, 2L),
      DISPUNIFORM = c(2L, 1L),
      DMONTH = c(4L, 4L),
      DIED = c(0L, 0L),
      I10_DX1 = c("I214", "I500")
    )

    DBI::dbWriteTable(con, "raw_nrd_no_sameday", dat)

    out <- dplyr::tbl(con, "raw_nrd_no_sameday") |>
      nrd_build_episodes() |>
      dplyr::collect()

    expect_true("Episode_SAMEDAYEVENT" %in% names(out))
    expect_true(is.na(out$Episode_SAMEDAYEVENT[[1]]))
  })
})

test_that("nrd_build_episodes preserves missingness for LOS and TOTCHG aggregates", {
  with_duckdb_connection(function(con) {
    dat <- tibble::tibble(
      YEAR = 2021L,
      NRD_VISITLINK = "C001",
      KEY_NRD = 4001L,
      NRD_DAYSTOEVENT = 20L,
      LOS = NA_real_,
      DISPUNIFORM = 1L,
      DMONTH = 1L,
      DIED = NA_integer_,
      I10_DX1 = "I214",
      TOTCHG = NA_real_,
      SAMEDAYEVENT = 0L
    )

    DBI::dbWriteTable(con, "raw_nrd_missing_episode_values", dat)

    out <- dplyr::tbl(con, "raw_nrd_missing_episode_values") |>
      nrd_build_episodes() |>
      dplyr::collect()

    expect_equal(nrow(out), 1)
    expect_true(is.na(out$Episode_LOS[[1]]))
    expect_true(is.na(out$Episode_TOTCHG[[1]]))
    expect_true(is.na(out$DIED[[1]]))
  })
})
