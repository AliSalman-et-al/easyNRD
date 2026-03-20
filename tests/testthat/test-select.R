test_that("nrd_select errors when YEAR, NRD_VISITLINK, or KEY_NRD is absent", {
  missing_year <- dplyr::tibble(NRD_VISITLINK = "A001", KEY_NRD = 1L)
  missing_visitlink <- dplyr::tibble(YEAR = 2019L, KEY_NRD = 1L)
  missing_key <- dplyr::tibble(YEAR = 2019L, NRD_VISITLINK = "A001")

  expect_error(nrd_select(missing_year), "YEAR")
  expect_error(nrd_select(missing_visitlink), "NRD_VISITLINK")
  expect_error(nrd_select(missing_key), "KEY_NRD")
})

test_that("nrd_select omits missing survey-design columns without error", {
  dat <- dplyr::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    KEY_NRD = 1L,
    value = 5L
  )

  expect_no_error(out <- nrd_select(dat, value))
  expect_identical(names(out), c("YEAR", "NRD_VISITLINK", "KEY_NRD", "value"))
})

test_that("nrd_select omits missing linkage-derived columns without error", {
  dat <- dplyr::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    KEY_NRD = 1L,
    extra = 99L
  )

  expect_no_error(out <- nrd_select(dat, extra))
  expect_identical(names(out), c("YEAR", "NRD_VISITLINK", "KEY_NRD", "extra"))
})

test_that("user-selected columns are appended after retained columns", {
  dat <- dplyr::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    KEY_NRD = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    IndexEvent = 1L,
    time_to_event = 7,
    outcome_status = "Readmitted",
    DMONTH = 1L,
    DIED = 0L,
    NRD_DAYSTOEVENT = 5L,
    LOS = 2L,
    custom_a = 10L,
    custom_b = 11L
  )

  out <- nrd_select(dat, custom_a, custom_b)

  expect_identical(
    names(out),
    c(
      "YEAR", "NRD_VISITLINK", "KEY_NRD", "HOSP_NRD", "DISCWT",
      "NRD_STRATUM", "IndexEvent", "time_to_event", "outcome_status",
      "DMONTH", "DIED", "NRD_DAYSTOEVENT", "LOS", "custom_a", "custom_b"
    )
  )
})

test_that("nrd_demographics and nrd_hospital_vars work as helpers inside nrd_select", {
  dat <- dplyr::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    KEY_NRD = 1L,
    AGE = 68L,
    FEMALE = 1L,
    ZIPINC_QRTL = 3L,
    PAY1 = 1L,
    RESIDENT = 1L,
    PL_NCHS = 4L,
    HOSP_BEDSIZE = "LARGE",
    H_CONTRL = 2L,
    HOSP_URCAT4 = 1L,
    HOSP_UR_TEACH = 3L
  )

  out <- nrd_select(dat, nrd_demographics(), nrd_hospital_vars())

  expect_true(all(nrd_demographics() %in% names(out)))
  expect_true(all(nrd_hospital_vars() %in% names(out)))
})

test_that("nrd_select works on both lazy DuckDB tables and in-memory data frames", {
  dat <- dplyr::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    KEY_NRD = 1L,
    HOSP_NRD = 101L,
    custom = 5L
  )

  in_memory <- nrd_select(dat, custom)
  expect_s3_class(in_memory, "data.frame")

  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  lazy <- nrd_ingest(path)
  on.exit(nrd_close(lazy), add = TRUE)

  lazy_out <- nrd_select(lazy, custom)
  expect_s3_class(lazy_out, "tbl_lazy")
  expect_equal(dplyr::collect(lazy_out), in_memory)
})
