test_that("nrd_select retains mandatory and conditional variables when present", {
  dat <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    Episode_KEY_NRD = 10001L,
    Episode_Admission_Day = 10L,
    Episode_Discharge_Day = 12L,
    Episode_DMONTH = 1L,
    DIED = 0L,
    IndexEvent = 1L,
    Readmit = 1L,
    DaysToReadmit = 7L,
    time_to_event = 12,
    outcome_status = "Readmitted",
    Episode_DX10 = "I214",
    Episode_PR10 = "02703ZZ",
    some_noise_var = 1
  )

  out <- dat |>
    nrd_select(some_noise_var)

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
    "Episode_KEY_NRD",
    "Episode_Admission_Day",
    "Episode_Discharge_Day",
    "Episode_DMONTH",
    "DIED",
    "DaysToReadmit",
    "Readmit",
    "some_noise_var"
  )

  expect_identical(names(out), expected)
  expect_false("Episode_DX10" %in% names(out))
  expect_false("Episode_PR10" %in% names(out))
})

test_that("nrd_select works when conditional variables are absent", {
  dat <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A0001",
    Episode_ID = 1L,
    HOSP_NRD = 101L,
    DISCWT = 2.5,
    NRD_STRATUM = 11L,
    Episode_DX10 = "I214",
    some_noise_var = 1
  )

  expect_no_error({
    out <- dat |>
      nrd_select(some_noise_var)
  })

  expected <- c(
    "YEAR",
    "NRD_VISITLINK",
    "Episode_ID",
    "HOSP_NRD",
    "DISCWT",
    "NRD_STRATUM",
    "some_noise_var"
  )

  expect_identical(names(out), expected)
  expect_false("Episode_DX10" %in% names(out))
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
