test_that("pattern mode returns TRUE for matching rows and FALSE otherwise", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1L, 2L),
    NRD_DaysToEvent = c(5L, 7L),
    LOS = c(2L, 3L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    I10_DX1 = c("I214", "J189")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$is_ami, c(TRUE, FALSE))
})

test_that("pattern mode principal scope matches only principal columns", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1L, 2L),
    NRD_DaysToEvent = c(5L, 7L),
    LOS = c(2L, 3L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    I10_DX1 = c("J189", "I214"),
    I10_DX2 = c("I219", "Z001")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "principal_only", pattern = "^I21", type = "dx", scope = "principal") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$principal_only, c(FALSE, TRUE))
})

test_that("pattern mode secondary scope matches only secondary columns", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1L, 2L),
    NRD_DaysToEvent = c(5L, 7L),
    LOS = c(2L, 3L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    I10_DX1 = c("I214", "J189"),
    I10_DX2 = c("Z001", "I219")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "secondary_only", pattern = "^I21", type = "dx", scope = "secondary") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$secondary_only, c(FALSE, TRUE))
})

test_that("pattern mode all scope matches across the full column array", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VisitLink = c("A001", "A002", "A003"),
    KEY_NRD = c(1L, 2L, 3L),
    NRD_DaysToEvent = c(5L, 7L, 9L),
    LOS = c(2L, 3L, 4L),
    DMONTH = c(1L, 1L, 1L),
    DIED = c(0L, 0L, 0L),
    I10_DX1 = c("J189", "J189", "I214"),
    I10_DX2 = c("I219", NA_character_, "Z001")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "all_dx", pattern = "^I21", type = "dx", scope = "all") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$all_dx, c(TRUE, FALSE, TRUE))
})

test_that("codes mode returns TRUE for exact matches and FALSE otherwise", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1L, 2L),
    NRD_DaysToEvent = c(5L, 7L),
    LOS = c(2L, 3L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    I10_PR1 = c("02703ZZ", "0FT44ZZ")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "pci", codes = "02703ZZ", type = "pr", scope = "principal") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$pci, c(TRUE, FALSE))
})

test_that("codes mode all scope matches across multiple columns", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VisitLink = c("A001", "A002", "A003"),
    KEY_NRD = c(1L, 2L, 3L),
    NRD_DaysToEvent = c(5L, 7L, 9L),
    LOS = c(2L, 3L, 4L),
    DMONTH = c(1L, 1L, 1L),
    DIED = c(0L, 0L, 0L),
    I10_PR1 = c("0FT44ZZ", "5A1955Z", NA_character_),
    I10_PR2 = c("02703ZZ", NA_character_, "0BH17EZ")
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "target_pr", codes = c("02703ZZ", "0BH17EZ"), type = "pr", scope = "all") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$target_pr, c(TRUE, FALSE, TRUE))
})

test_that("rows with all clinical columns NA produce FALSE", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(1L, 2L),
    NRD_DaysToEvent = c(5L, 7L),
    LOS = c(2L, 3L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    I10_DX1 = c(NA_character_, "I214"),
    I10_DX2 = c(NA_character_, NA_character_)
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$is_ami, c(FALSE, TRUE))
})

test_that("codes absent from the data produce FALSE for all rows", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "never_seen", codes = "ZZZZZZZ", type = "dx", scope = "all") |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_true(all(out$never_seen == FALSE))
})

test_that("supplying both pattern and codes errors clearly", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_error(
    nrd_flag_condition(data, "bad", pattern = "^I21", codes = "I214", type = "dx", scope = "all"),
    "Exactly one"
  )
})

test_that("supplying neither pattern nor codes errors clearly", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_error(
    nrd_flag_condition(data, "bad", type = "dx", scope = "all"),
    "Exactly one"
  )
})

test_that("invalid type or scope errors clearly", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_error(
    nrd_flag_condition(data, "bad_type", pattern = "^I21", type = "bad", scope = "all"),
    "type"
  )
  expect_error(
    nrd_flag_condition(data, "bad_scope", pattern = "^I21", type = "dx", scope = "bad"),
    "scope"
  )
})

test_that("pattern mode SQL does not contain IN clauses", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  sql <- dbplyr::sql_render(
    nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all")
  )

  expect_false(grepl("\\bIN\\s*\\(", sql, ignore.case = TRUE))
})

test_that("codes mode SQL does not contain regexp_matches", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  sql <- dbplyr::sql_render(
    nrd_flag_condition(data, "is_ami", codes = c("I214", "I219"), type = "dx", scope = "all")
  )

  expect_false(grepl("regexp_matches", sql, ignore.case = TRUE))
})

test_that("pattern mode SQL snapshot is stable", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  sql <- dbplyr::sql_render(
    nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all")
  )
  sql <- gsub("arrow_[0-9]+", "arrow_tbl", sql)

  expect_snapshot(cat(sql))
})

test_that("codes mode SQL snapshot is stable", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  sql <- dbplyr::sql_render(
    nrd_flag_condition(data, "is_ami", codes = c("I214", "I219"), type = "dx", scope = "all")
  )
  sql <- gsub("arrow_[0-9]+", "arrow_tbl", sql)

  expect_snapshot(cat(sql))
})

test_that("nrd_flag_condition stays lazy and does not collect", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all")

  expect_s3_class(out, "tbl_lazy")
  expect_true(inherits(dbplyr::remote_con(out), "duckdb_connection"))
  expect_false(inherits(out, c("data.frame", "tbl_df")))
})
