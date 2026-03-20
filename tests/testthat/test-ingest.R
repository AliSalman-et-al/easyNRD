test_that("single parquet path ingests to a DuckDB-backed lazy table", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_s3_class(data, "tbl_lazy")
  expect_true(inherits(dbplyr::remote_con(data), "duckdb_connection"))
  expect_false(inherits(data, "data.frame"))
})

test_that("multiple parquet paths unify schemas correctly", {
  path_a <- make_synthetic_nrd(tibble::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 1001L,
    HOSP_NRD = 10L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L,
    I10_DX1 = "I2101"
  ))
  path_b <- make_synthetic_nrd(tibble::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A002",
    KEY_NRD = 1002L,
    HOSP_NRD = 11L,
    DISCWT = 1,
    NRD_STRATUM = 102L,
    NRD_DaysToEvent = 8L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L,
    I10_DX1 = "J189",
    EXTRA_COL = "present_only_in_second"
  ))
  on.exit(unlink(c(path_a, path_b)), add = TRUE)

  data <- nrd_ingest(c(path_a, path_b))
  on.exit(nrd_close(data), add = TRUE)

  cols <- colnames(data)
  expect_true("EXTRA_COL" %in% cols)

  rows <- dplyr::collect(dplyr::arrange(data, KEY_NRD))
  expect_equal(nrow(rows), 2L)
  expect_true(is.na(rows$EXTRA_COL[[1]]))
  expect_identical(rows$EXTRA_COL[[2]], "present_only_in_second")
})

test_that("NRD_VisitLink is renamed to NRD_VISITLINK", {
  path <- make_synthetic_nrd(tibble::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 1001L,
    HOSP_NRD = 10L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_true("NRD_VISITLINK" %in% colnames(data))
  expect_false("NRD_VisitLink" %in% colnames(data))
})

test_that("NRD_DaysToEvent is renamed to NRD_DAYSTOEVENT", {
  path <- make_synthetic_nrd(tibble::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 1001L,
    HOSP_NRD = 10L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_true("NRD_DAYSTOEVENT" %in% colnames(data))
  expect_false("NRD_DaysToEvent" %in% colnames(data))
})

test_that("passing an already-lazy DuckDB table is a clean pass-through", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  original <- nrd_ingest(path)
  on.exit(nrd_close(original), add = TRUE)

  passthrough <- nrd_ingest(original)

  expect_identical(passthrough, original)
})

test_that("fresh DuckDB connection is configured with easyNRD temp directory", {
  local_cache <- tempfile("easynrd-cache-")
  old_option <- getOption("easynrd.cache_dir")
  options(easynrd.cache_dir = local_cache)
  on.exit(options(easynrd.cache_dir = old_option), add = TRUE)

  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  con <- dbplyr::remote_con(data)
  temp_dir <- DBI::dbGetQuery(
    con,
    "SELECT current_setting('temp_directory') AS temp_directory"
  )$temp_directory[[1]]

  expect_true(startsWith(normalizePath(temp_dir, winslash = "/"), normalizePath(nrd_cache_dir(), winslash = "/")))
  expect_match(basename(temp_dir), paste0("^easyNRD_", Sys.getpid(), "_"))
})

test_that("preserve_insertion_order is FALSE on new ingest connections", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  con <- dbplyr::remote_con(data)
  setting <- DBI::dbGetQuery(
    con,
    "SELECT current_setting('preserve_insertion_order') AS preserve_insertion_order"
  )$preserve_insertion_order[[1]]

  expect_true(identical(setting, FALSE) || identical(setting, "false"))
})

test_that("nrd_close disconnects the connection and removes the session temp directory", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  con <- dbplyr::remote_con(data)
  temp_dir <- DBI::dbGetQuery(
    con,
    "SELECT current_setting('temp_directory') AS temp_directory"
  )$temp_directory[[1]]

  expect_true(DBI::dbIsValid(con))
  expect_true(dir.exists(temp_dir))

  expect_true(nrd_close(data))
  expect_false(DBI::dbIsValid(con))
  expect_false(dir.exists(temp_dir))
  expect_false(nrd_close(data))
})

test_that("ingest result is never an in-memory object", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_false(inherits(data, c("tbl_df", "data.frame")))
  expect_true(inherits(dbplyr::remote_con(data), "duckdb_connection"))
})
