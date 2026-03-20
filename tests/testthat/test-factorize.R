test_that("nrd_factorize converts dictionary columns to ordered factors", {
  dat <- dplyr::tibble(
    FEMALE = c("Female", "Male", NA_character_),
    PAY1 = c("Private", "Medicare", "Other"),
    AGE = c(55L, 62L, 71L)
  )

  out <- nrd_factorize(dat)
  dict <- get("nrd_dict", envir = asNamespace("easyNRD"))

  expect_true(is.factor(out$FEMALE))
  expect_true(is.factor(out$PAY1))
  expect_false(is.factor(out$AGE))
  expect_identical(levels(out$FEMALE), unname(dict$FEMALE))
  expect_identical(levels(out$PAY1), unname(dict$PAY1))
})

test_that("nrd_factorize ignores columns not in dictionary", {
  dat <- dplyr::tibble(
    AGE = c(45L, 50L),
    is_ami = c(1L, 0L)
  )

  out <- nrd_factorize(dat)

  expect_identical(out, dat)
})

test_that("nrd_factorize handles all-NA dictionary columns", {
  dat <- dplyr::tibble(
    FEMALE = c(NA_character_, NA_character_),
    AGE = c(40L, 41L)
  )

  out <- nrd_factorize(dat)
  dict <- get("nrd_dict", envir = asNamespace("easyNRD"))

  expect_true(is.factor(out$FEMALE))
  expect_true(all(is.na(out$FEMALE)))
  expect_identical(levels(out$FEMALE), unname(dict$FEMALE))
})

test_that("nrd_factorize works with in-memory tbl_svy", {
  dat <- dplyr::tibble(
    HOSP_NRD = c(101L, 102L),
    DISCWT = c(1.5, 2.0),
    NRD_STRATUM = c(11L, 11L),
    FEMALE = c("Male", "Female"),
    AGE = c(66L, 72L)
  )

  svy <- nrd_as_survey(dat)
  out <- nrd_factorize(svy)
  dict <- get("nrd_dict", envir = asNamespace("easyNRD"))

  expect_s3_class(out, "tbl_svy")
  expect_true(is.factor(out$variables$FEMALE))
  expect_identical(levels(out$variables$FEMALE), unname(dict$FEMALE))
})

test_that("nrd_factorize prioritizes _label columns from nrd_label keep_original", {
  dat <- dplyr::tibble(
    FEMALE = c(0L, 1L, 0L),
    PAY1 = c(1L, 3L, 6L),
    AGE = c(50L, 63L, 77L)
  )

  labeled <- nrd_label(dat, FEMALE, .keep_original = TRUE)
  out <- nrd_factorize(labeled)
  dict <- get("nrd_dict", envir = asNamespace("easyNRD"))

  expect_type(out$FEMALE, "integer")
  expect_true(is.factor(out$FEMALE_label))
  expect_identical(levels(out$FEMALE_label), unname(dict$FEMALE))
  expect_identical(as.character(out$FEMALE_label), c("Male", "Female", "Male"))
})

test_that("nrd_factorize errors clearly on lazy tables", {
  path <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 1L,
    NRD_DaysToEvent = 5L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L,
    FEMALE = 1L
  ))
  on.exit(unlink(path), add = TRUE)

  lazy <- nrd_ingest(path)
  on.exit(nrd_close(lazy), add = TRUE)

  expect_error(
    nrd_factorize(lazy),
    "in-memory|collect"
  )
})
