test_that("nrd_select retains mandatory variables", {
  dat <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    IndexEvent = 1L,
    time_to_event = 12,
    outcome_status = "Readmitted",
    dummy_001 = 1,
    dummy_002 = 2,
    dummy_003 = 3
  )

  out <- dat |>
    nrd_select(dummy_001)

  expected <- c(
    "YEAR",
    "NRD_VISITLINK",
    "Episode_ID",
    "HOSP_NRD",
    "DISCWT",
    "NRD_STRATUM",
    "IndexEvent",
    "time_to_event",
    "outcome_status",
    "dummy_001"
  )

  expect_setequal(names(out), expected)
})

test_that("nrd_select conditionally retains linkage variables", {
  dat_with_linkage <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    IndexEvent = 1L,
    time_to_event = 12,
    outcome_status = "Readmitted",
    dummy_001 = 1
  )

  dat_without_linkage <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    dummy_001 = 1
  )

  out_with_linkage <- dat_with_linkage |>
    nrd_select(dummy_001)

  expect_true(all(c("IndexEvent", "time_to_event", "outcome_status") %in% names(out_with_linkage)))

  expect_no_error({
    out_without_linkage <- dat_without_linkage |>
      nrd_select(dummy_001)
  })

  expect_false(any(c("IndexEvent", "time_to_event", "outcome_status") %in% names(out_without_linkage)))
})

test_that("nrd_demographics and nrd_hospital_vars work in nrd_select", {
  dat <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    AGE = 68L,
    FEMALE = 1L,
    ZIPINC_QRTL = 3L,
    PAY1 = 1L,
    RESIDENT = 1L,
    PL_NCHS = 4L,
    HOSP_BEDSIZE = "LARGE",
    H_CONTRL = 2L,
    HOSP_URCAT4 = 1L,
    HOSP_UR_TEACH = 3L,
    dummy_001 = 1
  )

  out <- dat |>
    nrd_select(nrd_demographics(), nrd_hospital_vars())

  expect_true(all(nrd_demographics() %in% names(out)))
  expect_true(all(nrd_hospital_vars() %in% names(out)))
})
