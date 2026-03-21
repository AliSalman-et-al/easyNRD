test_that("denominator is preserved and year scope is respected", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2020L),
    NRD_VISITLINK = c("A001", "A001"),
    KEY_NRD = c(101L, 201L),
    NRD_DaysToEvent = c(10L, 15L),
    LOS = c(2L, 3L),
    DMONTH = c(11L, 1L),
    DIED = c(0L, 0L),
    is_index = c(1L, 1L),
    is_readmit = c(1L, 1L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L
  ) |>
    dplyr::arrange(YEAR, KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$outcome_status[out$KEY_NRD == 101L], "Censored")
  expect_true(is.na(out$outcome_status[out$KEY_NRD == 201L]) || out$outcome_status[out$KEY_NRD == 201L] %in% c("Censored", "Readmitted"))
})

test_that("time_to_event uses the exact HCUP gap formula", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 20L),
    LOS = c(3L, 2L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    is_index = c(1L, 1L),
    is_readmit = c(1L, 1L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L,
    readmit_vars = KEY_NRD
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$IndexEvent, c(1L, 1L))
  expect_identical(out$time_to_event[[1]], 7)
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
})

test_that("only the first qualifying readmission is linked", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "A001"),
    KEY_NRD = c(101L, 102L, 103L),
    NRD_DaysToEvent = c(10L, 18L, 25L),
    LOS = c(2L, 1L, 1L),
    DMONTH = c(1L, 1L, 1L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    is_readmit = c(1L, 1L, 1L),
    severity = c("index", "first", "later")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L,
    readmit_vars = c(KEY_NRD, severity)
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_identical(out$readmit_severity[[1]], "first")
  expect_identical(out$time_to_event[[1]], 6)
})

test_that("tie-breaking falls through to smaller KEY_NRD when gap and day tie", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "A001"),
    KEY_NRD = c(101L, 102L, 103L),
    NRD_DaysToEvent = c(10L, 20L, 20L),
    LOS = c(2L, 1L, 1L),
    DMONTH = c(1L, 1L, 1L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    is_readmit = c(1L, 1L, 1L),
    marker = c("index", "smaller_key", "larger_key")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L,
    readmit_vars = c(KEY_NRD, marker)
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_identical(out$readmit_marker[[1]], "smaller_key")
})

test_that("index censoring uses the DMONTH threshold", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A002"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 15L),
    LOS = c(2L, 2L),
    DMONTH = c(12L, 11L),
    DIED = c(0L, 0L),
    is_index = c(1L, 1L),
    is_readmit = c(0L, 0L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$IndexEvent, c(0L, 1L))
})

test_that("death at index is excluded and labeled correctly", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A002"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 20L),
    LOS = c(2L, 2L),
    DMONTH = c(1L, 1L),
    DIED = c(1L, 0L),
    is_index = c(1L, 0L),
    is_readmit = c(0L, 0L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$IndexEvent[[1]], 0L)
  expect_identical(out$outcome_status[[1]], "Died at Index")
  expect_identical(out$time_to_event[[1]], 0)
})

test_that("non-index rows have NA outcomes and censored rows have non-missing time", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A002"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 20L),
    LOS = c(2L, 2L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    is_index = c(0L, 1L),
    is_readmit = c(0L, 0L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path) |>
    nrd_prepare()
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$IndexEvent[[1]], 0L)
  expect_true(is.na(out$time_to_event[[1]]))
  expect_true(is.na(out$outcome_status[[1]]))
  expect_identical(out$outcome_status[[2]], "Censored")
  expect_false(is.na(out$time_to_event[[2]]))
})

test_that("readmit_vars are prefixed and remain NA for non-readmitted rows", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "B001"),
    KEY_NRD = c(101L, 102L, 201L),
    NRD_DaysToEvent = c(10L, 18L, 30L),
    LOS = c(2L, 1L, 2L),
    DMONTH = c(1L, 1L, 1L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    is_readmit = c(1L, 1L, 0L),
    severity = c("index", "readmit", "censored")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L,
    readmit_vars = c(KEY_NRD, severity)
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_true(all(c("readmit_KEY_NRD", "readmit_severity") %in% names(out)))
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_true(is.na(out$readmit_KEY_NRD[[3]]))
  expect_true(is.na(out$readmit_severity[[3]]))
})

test_that("readmissions can become later index events", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "A001"),
    KEY_NRD = c(101L, 102L, 103L),
    NRD_DaysToEvent = c(10L, 20L, 35L),
    LOS = c(2L, 2L, 2L),
    DMONTH = c(1L, 1L, 2L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    is_readmit = c(1L, 1L, 1L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = is_readmit == 1L,
    window = 30L,
    readmit_vars = KEY_NRD
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_identical(out$readmit_KEY_NRD[[2]], 103L)
  expect_identical(out$outcome_status[[2]], "Readmitted")
})

test_that("final linkage preserves original columns after narrowing the checkpoint", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "B001"),
    KEY_NRD = c(101L, 102L, 201L),
    NRD_DaysToEvent = c(10L, 18L, 40L),
    LOS = c(2L, 1L, 2L),
    DMONTH = c(1L, 1L, 2L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    trigger_readmit = c(0L, 1L, 0L),
    severity = c("index", "readmit", "other"),
    retained_marker = c("keep_a", "keep_b", "keep_c")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = trigger_readmit == 1L,
    window = 30L,
    readmit_vars = KEY_NRD
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(out), nrow(dat))
  expect_true(all(c("severity", "retained_marker", "trigger_readmit") %in% names(out)))
  expect_false(".nrd_readmit_eligible" %in% names(out))
  expect_identical(out$retained_marker, dat$retained_marker)
  expect_identical(out$severity, dat$severity)
  expect_identical(out$trigger_readmit, dat$trigger_readmit)
})

test_that("readmit_condition is resolved before the narrowed checkpoint", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "B001"),
    KEY_NRD = c(101L, 102L, 201L),
    NRD_DaysToEvent = c(10L, 18L, 40L),
    LOS = c(2L, 1L, 2L),
    DMONTH = c(1L, 1L, 2L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    trigger_readmit = c(0L, 1L, 0L),
    severity = c("index", "readmit", "other")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = trigger_readmit == 1L,
    window = 30L,
    readmit_vars = KEY_NRD
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_true(is.na(out$readmit_KEY_NRD[[2]]))
})

test_that("requested readmit_vars are retained through the narrowed checkpoint", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001", "A001"),
    KEY_NRD = c(101L, 102L, 103L),
    NRD_DaysToEvent = c(10L, 18L, 30L),
    LOS = c(2L, 1L, 1L),
    DMONTH = c(1L, 1L, 2L),
    DIED = c(0L, 0L, 0L),
    is_index = c(1L, 1L, 1L),
    trigger_readmit = c(0L, 1L, 1L),
    severity = c("index", "first", "later"),
    retained_marker = c("keep_a", "keep_b", "keep_c")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- nrd_link_readmissions(
    data,
    index_condition = is_index == 1L,
    readmit_condition = trigger_readmit == 1L,
    window = 30L,
    readmit_vars = c(KEY_NRD, severity)
  ) |>
    dplyr::arrange(KEY_NRD) |>
    dplyr::collect()

  expect_true(all(c("readmit_KEY_NRD", "readmit_severity") %in% names(out)))
  expect_identical(out$readmit_KEY_NRD[[1]], 102L)
  expect_identical(out$readmit_severity[[1]], "first")
})

test_that("nrd_link_readmissions stays lazy and uses a DuckDB compute checkpoint", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 18L),
    LOS = c(2L, 1L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    is_index = c(1L, 1L),
    is_readmit = c(1L, 1L)
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  compute_calls <- 0L
  original_checkpoint <- .nrd_compute_checkpoint
  old_verbose <- Sys.getenv("EASYNRD_VERBOSE", unset = NA_character_)
  Sys.unsetenv("EASYNRD_VERBOSE")
  on.exit(
    {
      if (is.na(old_verbose)) {
        Sys.unsetenv("EASYNRD_VERBOSE")
      } else {
        Sys.setenv(EASYNRD_VERBOSE = old_verbose)
      }
    },
    add = TRUE
  )

  out <- testthat::with_mocked_bindings(
    testthat::with_mocked_bindings(
      nrd_link_readmissions(
        data,
        index_condition = is_index == 1L,
        readmit_condition = is_readmit == 1L,
        window = 30L
      ),
      collect = function(...) {
        stop("collect should not be called")
      },
      .package = "dplyr"
    ),
    .nrd_compute_checkpoint = function(x, label, ...) {
      compute_calls <<- compute_calls + 1L
      result <- original_checkpoint(x, label = label, ...)
      expect_s3_class(result, "tbl_lazy")
      expect_true(inherits(dbplyr::remote_con(result), "duckdb_connection"))
      result
    },
    .package = "easyNRD"
  )

  expect_equal(compute_calls, 3L)
  expect_s3_class(out, "tbl_lazy")
  expect_true(inherits(dbplyr::remote_con(out), "duckdb_connection"))
})

test_that("final SQL snapshot is stable", {
  dat <- dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VISITLINK = c("A001", "A001"),
    KEY_NRD = c(101L, 102L),
    NRD_DaysToEvent = c(10L, 18L),
    LOS = c(2L, 1L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L),
    is_index = c(1L, 1L),
    is_readmit = c(1L, 1L),
    severity = c("index", "readmit")
  )
  path <- make_synthetic_nrd(dat)
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  sql <- dbplyr::sql_render(
    nrd_link_readmissions(
      data,
      index_condition = is_index == 1L,
      readmit_condition = is_readmit == 1L,
      window = 30L,
      readmit_vars = c(KEY_NRD, severity)
    )
  )

  sql <- gsub("arrow_[0-9]+", "arrow_tbl", sql)
  sql <- gsub("dbplyr_[A-Za-z0-9_]+", "checkpoint_tbl", sql)

  expect_snapshot(cat(sql))
})
