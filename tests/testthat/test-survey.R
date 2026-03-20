test_that("nrd_as_survey maps HCUP survey design variables", {
  dat <- tibble::tibble(
    HOSP_NRD = c(201L, 202L, 203L, 204L),
    DISCWT = c(0.7, 1.1, 2.4, 0.8),
    NRD_STRATUM = c(21L, 21L, 22L, 22L),
    FEMALE = c(1L, 0L, 1L, 0L)
  )

  out <- nrd_as_survey(dat)

  expect_s3_class(out, "tbl_svy")
  expect_identical(as.character(stats::formula(out$cluster))[2], "HOSP_NRD")
  expect_identical(as.character(stats::formula(out$strata))[2], "NRD_STRATUM")
  expect_equal(as.numeric(stats::weights(out)), dat$DISCWT)
  expect_equal(
    survey::degf(out),
    dplyr::n_distinct(dat$HOSP_NRD) - dplyr::n_distinct(dat$NRD_STRATUM)
  )
})

test_that("domain estimates from nrd_as_survey agree with survey::subset workflow", {
  dat <- tibble::tibble(
    HOSP_NRD = c(101L, 101L, 102L, 102L, 103L, 103L),
    NRD_STRATUM = c(1L, 1L, 1L, 1L, 2L, 2L),
    DISCWT = c(2.0, 1.0, 1.5, 1.0, 3.0, 0.5),
    IndexEvent = c(1L, 0L, 1L, 1L, 0L, 0L),
    outcome_status = c("Readmitted", "Censored", "Readmitted", "Censored", "Censored", "Readmitted")
  )
  dat$readmitted <- as.numeric(dat$outcome_status == "Readmitted")

  easy_domain <- dat |>
    nrd_as_survey() |>
    srvyr::filter(IndexEvent == 1L) |>
    dplyr::summarise(
      readmit_rate = srvyr::survey_mean(readmitted, vartype = "se")
    )

  ref_design <- survey::svydesign(
    ids = ~HOSP_NRD,
    strata = ~NRD_STRATUM,
    weights = ~DISCWT,
    data = dat,
    nest = TRUE
  )
  ref_domain <- subset(ref_design, IndexEvent == 1L)
  ref_rate <- survey::svymean(~readmitted, ref_domain)

  expect_equal(easy_domain$readmit_rate, as.numeric(stats::coef(ref_rate)[[1]]), tolerance = 1e-8)
  expect_equal(easy_domain$readmit_rate_se, as.numeric(survey::SE(ref_rate)[[1]]), tolerance = 1e-8)
})

test_that("pre-filtering before survey design can yield invalid variance estimation", {
  dat <- tibble::tibble(
    HOSP_NRD = c(501L, 501L, 502L, 502L, 503L, 503L, 504L, 504L),
    NRD_STRATUM = c(10L, 10L, 10L, 10L, 20L, 20L, 20L, 20L),
    DISCWT = c(1.0, 2.5, 0.8, 1.8, 3.0, 0.7, 1.3, 2.1),
    IndexEvent = c(1L, 1L, 0L, 0L, 1L, 1L, 0L, 0L),
    outcome_status = c("Readmitted", "Censored", "Readmitted", "Censored", "Readmitted", "Censored", "Censored", "Readmitted")
  )
  dat$readmitted <- as.numeric(dat$outcome_status == "Readmitted")

  proper <- dat |>
    nrd_as_survey() |>
    srvyr::filter(IndexEvent == 1L) |>
    dplyr::summarise(
      readmit_rate = srvyr::survey_mean(readmitted, vartype = "se")
    )

  naive_data <- dplyr::filter(dat, IndexEvent == 1L)
  naive_design <- survey::svydesign(
    ids = ~HOSP_NRD,
    strata = ~NRD_STRATUM,
    weights = ~DISCWT,
    data = naive_data,
    nest = TRUE
  )

  expect_gt(proper$readmit_rate_se, 0)
  expect_error(
    survey::svymean(~readmitted, naive_design),
    "PSU|stratum|lonely",
    ignore.case = TRUE
  )
})

test_that("nrd_as_survey preserves lazy backend for duckdb tables", {
  with_duckdb_connection(function(con) {
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
    out <- nrd_as_survey(lazy_tbl)

    expect_s3_class(out, "tbl_svy")
    expect_s3_class(out$variables, "tbl_lazy")
    expect_equal(survey::degf(out), 1)
  })
})
