test_that("full easyNRD pipeline works end to end on a synthetic parquet fixture", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L, 2019L),
    NRD_VisitLink = c("A001", "A001", "B001", "C001"),
    KEY_NRD = c(1001L, 1002L, 1003L, 1004L),
    HOSP_NRD = c(11L, 12L, 13L, 14L),
    DISCWT = c(1.0, 1.2, 0.8, 1.5),
    NRD_STRATUM = c(101L, 101L, 102L, 103L),
    NRD_DaysToEvent = c(5L, 20L, 40L, 60L),
    LOS = c(3L, 2L, 4L, 5L),
    DMONTH = c(1L, 2L, 11L, 12L),
    DIED = c(0L, 0L, 0L, 0L),
    FEMALE = c(1L, 1L, 0L, 1L),
    I10_DX1 = c("I2101", "Z001", "I219", "K359"),
    I10_DX2 = c(NA_character_, "I214", NA_character_, NA_character_),
    I10_PR1 = c("02703ZZ", "0FT44ZZ", "02703ZZ", "5A1955Z"),
    I10_PR2 = c(NA_character_, NA_character_, NA_character_, NA_character_)
  ))
  on.exit(unlink(path), add = TRUE)

  export_path <- tempfile(fileext = ".parquet")
  on.exit(unlink(export_path), add = TRUE)

  data <- nrd_ingest(path)
  reingested <- NULL
  on.exit({
    if (!is.null(reingested)) {
      nrd_close(reingested)
    }
    nrd_close(data)
  }, add = TRUE)

  pipeline <- data |>
    nrd_prepare() |>
    nrd_flag_condition("is_ami", pattern = "^I21", type = "dx", scope = "all") |>
    nrd_flag_condition("has_revascularization", codes = c("02703ZZ"), type = "pr", scope = "all") |>
    dplyr::mutate(custom_risk = dplyr::if_else(FEMALE == 1L, "higher", "lower")) |>
    nrd_link_readmissions(
      index_condition = is_ami,
      readmit_condition = has_revascularization,
      readmit_vars = c(HOSP_NRD, custom_risk)
    ) |>
    nrd_select(is_ami, has_revascularization, custom_risk, readmit_HOSP_NRD, readmit_custom_risk)

  row_count_before <- pipeline |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::pull(n)

  expect_equal(row_count_before, 4L)

  expect_no_error(nrd_export(pipeline, export_path))
  expect_true(file.exists(export_path))

  reingested <- nrd_ingest(export_path)

  row_count_after <- reingested |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::pull(n)

  expect_equal(row_count_after, row_count_before)

  exported_names <- colnames(reingested)
  expect_true(all(c(
    "YEAR", "NRD_VISITLINK", "KEY_NRD", "HOSP_NRD", "DISCWT", "NRD_STRATUM",
    "IndexEvent", "time_to_event", "outcome_status", "Discharge_Day",
    "is_ami", "has_revascularization", "custom_risk", "readmit_HOSP_NRD",
    "readmit_custom_risk"
  ) %in% exported_names))

  survey_design <- nrd_as_survey(reingested)

  expect_s3_class(survey_design, "tbl_svy")
  expect_identical(as.character(stats::formula(survey_design$cluster))[2], "HOSP_NRD")
  expect_identical(as.character(stats::formula(survey_design$strata))[2], "NRD_STRATUM")
  expect_equal(
    as.numeric(stats::weights(survey_design)),
    reingested |> dplyr::collect() |> dplyr::pull(DISCWT)
  )

  expect_true(nrd_close(reingested))
  reingested <- NULL
  expect_true(nrd_close(data))
})
