test_that("nrd_factorize converts dictionary columns to ordered factors", {
  dat <- tibble::tibble(
    FEMALE = c("Female", "Male", NA_character_),
    PAY1 = c("Private", "Medicare", "Other"),
    AGE = c(55L, 62L, 71L)
  )

  out <- nrd_factorize(dat)

  expect_true(is.factor(out$FEMALE))
  expect_true(is.factor(out$PAY1))
  expect_false(is.factor(out$AGE))
  expect_identical(levels(out$FEMALE), unname(easyNRD:::nrd_dict$FEMALE))
  expect_identical(levels(out$PAY1), unname(easyNRD:::nrd_dict$PAY1))
})

test_that("nrd_factorize ignores columns not in dictionary", {
  dat <- tibble::tibble(
    AGE = c(45L, 50L),
    is_ami = c(1L, 0L)
  )

  out <- nrd_factorize(dat)

  expect_identical(out, dat)
})

test_that("nrd_factorize handles all-NA dictionary columns", {
  dat <- tibble::tibble(
    FEMALE = c(NA_character_, NA_character_),
    AGE = c(40L, 41L)
  )

  out <- nrd_factorize(dat)

  expect_true(is.factor(out$FEMALE))
  expect_true(all(is.na(out$FEMALE)))
  expect_identical(levels(out$FEMALE), unname(easyNRD:::nrd_dict$FEMALE))
})

test_that("nrd_factorize works with in-memory tbl_svy", {
  dat <- tibble::tibble(
    HOSP_NRD = c(101L, 102L),
    DISCWT = c(1.5, 2.0),
    NRD_STRATUM = c(11L, 11L),
    FEMALE = c("Male", "Female"),
    AGE = c(66L, 72L)
  )

  svy <- nrd_as_survey(dat)
  out <- nrd_factorize(svy)

  expect_s3_class(out, "tbl_svy")
  expect_true(is.factor(out$variables$FEMALE))
  expect_identical(levels(out$variables$FEMALE), unname(easyNRD:::nrd_dict$FEMALE))
})

test_that("nrd_factorize prioritizes _label columns from nrd_label keep_original", {
  dat <- tibble::tibble(
    FEMALE = c(0L, 1L, 0L),
    PAY1 = c(1L, 3L, 6L),
    AGE = c(50L, 63L, 77L)
  )

  labeled <- nrd_label(dat, FEMALE, .keep_original = TRUE)
  out <- nrd_factorize(labeled)

  expect_type(out$FEMALE, "integer")
  expect_true(is.factor(out$FEMALE_label))
  expect_identical(levels(out$FEMALE_label), unname(easyNRD:::nrd_dict$FEMALE))
  expect_identical(as.character(out$FEMALE_label), c("Male", "Female", "Male"))
})
