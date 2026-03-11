test_that("nrd_as_survey returns a tbl_svy object", {
  dat <- tibble::tibble(
    HOSP_NRD = c(101L, 102L, 103L),
    DISCWT = c(1.5, 2.0, 3.0),
    NRD_STRATUM = c(11L, 11L, 12L),
    AGE = c(65L, 72L, 58L),
    is_ami = c(1L, 0L, 1L)
  )

  out <- nrd_as_survey(dat)

  expect_s3_class(out, "tbl_svy")
})

test_that("nrd_as_survey maps cluster, weights, and strata", {
  dat <- tibble::tibble(
    HOSP_NRD = c(201L, 202L, 203L),
    DISCWT = c(0.7, 1.1, 2.4),
    NRD_STRATUM = c(21L, 21L, 22L),
    FEMALE = c(1L, 0L, 1L)
  )

  out <- nrd_as_survey(dat)

  expect_identical(as.character(stats::formula(out$cluster))[2], "HOSP_NRD")
  expect_identical(as.character(stats::formula(out$strata))[2], "NRD_STRATUM")
  expect_equal(as.numeric(stats::weights(out)), dat$DISCWT)
  expect_equal(survey::degf(out), dplyr::n_distinct(dat$HOSP_NRD) - dplyr::n_distinct(dat$NRD_STRATUM))
})

test_that("nrd_as_survey preserves lazy backend for duckdb tables", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  DBI::dbWriteTable(
    con,
    "nrd_mock",
    tibble::tibble(
      HOSP_NRD = c(301L, 302L),
      DISCWT = c(1.0, 1.8),
      NRD_STRATUM = c(31L, 31L),
      diagnosis_flag = c(0L, 1L)
    )
  )

  lazy_tbl <- dplyr::tbl(con, "nrd_mock")

  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  out <- nrd_as_survey(lazy_tbl)

  expect_s3_class(out, "tbl_svy")
  expect_s3_class(out$variables, "tbl_lazy")
  expect_equal(survey::degf(out), 1)
})
