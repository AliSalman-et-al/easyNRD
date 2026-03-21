test_that("nrd_pipeline pools two yearly pipelines lazily", {
  path_2018 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2018L, 2018L),
    NRD_VisitLink = c("A001", "A002"),
    KEY_NRD = c(101L, 102L),
    HOSP_NRD = c(11L, 12L),
    DISCWT = c(1.0, 1.2),
    NRD_STRATUM = c(101L, 102L),
    NRD_DaysToEvent = c(5L, 12L),
    LOS = c(2L, 4L),
    DMONTH = c(1L, 1L),
    DIED = c(0L, 0L)
  ))
  path_2019 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = c(2019L, 2019L),
    NRD_VisitLink = c("B001", "B002"),
    KEY_NRD = c(201L, 202L),
    HOSP_NRD = c(21L, 22L),
    DISCWT = c(0.8, 1.1),
    NRD_STRATUM = c(201L, 202L),
    NRD_DaysToEvent = c(7L, 15L),
    LOS = c(3L, 2L),
    DMONTH = c(1L, 2L),
    DIED = c(0L, 0L)
  ))
  on.exit(unlink(c(path_2018, path_2019)), add = TRUE)

  pooled <- nrd_pipeline(
    c("2018" = path_2018, "2019" = path_2019),
    function(data) {
      data |>
        nrd_prepare() |>
        dplyr::mutate(is_alive = DIED == 0L)
    }
  )
  on.exit(nrd_close(pooled), add = TRUE)

  expect_s3_class(pooled, "tbl_lazy")
  expect_true(inherits(dbplyr::remote_con(pooled), "duckdb_connection"))

  rows <- pooled |>
    dplyr::arrange(YEAR, KEY_NRD) |>
    dplyr::collect()

  expect_equal(nrow(rows), 4L)
  expect_equal(as.integer(rows$YEAR), c(2018L, 2018L, 2019L, 2019L))
  expect_true(all(c("Discharge_Day", "is_alive") %in% names(rows)))
  expect_true(all(rows$is_alive))
})

test_that("nrd_pipeline preserves supplied year order in pooled ingest", {
  path_a <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "A001",
    KEY_NRD = 101L,
    HOSP_NRD = 11L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L
  ))
  path_b <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2018L,
    NRD_VisitLink = "B001",
    KEY_NRD = 201L,
    HOSP_NRD = 21L,
    DISCWT = 1,
    NRD_STRATUM = 201L,
    NRD_DaysToEvent = 8L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(c(path_a, path_b)), add = TRUE)

  ingest_calls <- list()
  original_ingest <- nrd_ingest

  pooled <- testthat::with_mocked_bindings(
    nrd_pipeline(
      c("2019" = path_a, "2018" = path_b),
      function(data) dplyr::filter(data, DIED == 0L)
    ),
    nrd_ingest = function(paths) {
      ingest_calls[[length(ingest_calls) + 1L]] <<- paths
      original_ingest(paths)
    },
    .package = "easyNRD"
  )
  on.exit(nrd_close(pooled), add = TRUE)

  expect_length(ingest_calls, 3L)
  expect_identical(unname(ingest_calls[[1L]]), path_a)
  expect_identical(unname(ingest_calls[[2L]]), path_b)
  expect_equal(basename(unname(ingest_calls[[3L]])), c("NRD_2019_pipeline.parquet", "NRD_2018_pipeline.parquet"))
})

test_that("nrd_pipeline rethrows yearly errors with parent context", {
  path_2018 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2018L,
    NRD_VisitLink = "A001",
    KEY_NRD = 101L,
    HOSP_NRD = 11L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L
  ))
  path_2019 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "B001",
    KEY_NRD = 201L,
    HOSP_NRD = 21L,
    DISCWT = 1,
    NRD_STRATUM = 201L,
    NRD_DaysToEvent = 8L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(c(path_2018, path_2019)), add = TRUE)

  expect_error(
    nrd_pipeline(
      c("2018" = path_2018, "2019" = path_2019),
      function(data) {
        year_value <- data |>
          dplyr::summarise(year = dplyr::first(YEAR)) |>
          dplyr::pull(year)

        if (identical(as.integer(year_value), 2019L)) {
          rlang::abort("boom")
        }

        data
      }
    ),
    regexp = "`nrd_pipeline\\(\\)` failed for year `2019`\\..*boom"
  )
})

test_that("nrd_pipeline rejects collected results from .f", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  expect_error(
    nrd_pipeline(
      c("2019" = path),
      function(data) dplyr::collect(data)
    ),
    regexp = "do not call `collect\\(\\)` inside `.f`"
  )
})

test_that("nrd_pipeline rejects Arrow-backed results from .f", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  expect_error(
    nrd_pipeline(
      c("2019" = path),
      function(data) {
        arrow::open_dataset(path, format = "parquet") |>
          dplyr::filter(YEAR == 2019L)
      }
    ),
    regexp = "DuckDB-backed lazy table"
  )
})

test_that("nrd_pipeline validates inputs clearly", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  expect_error(
    nrd_pipeline(character(0), identity),
    regexp = "non-empty named character vector or named list"
  )
  expect_error(
    nrd_pipeline(c(path), identity),
    regexp = "non-missing, non-empty names"
  )
  expect_error(
    nrd_pipeline(c("2019" = path, "2019" = path), identity),
    regexp = "unique names"
  )
  expect_error(
    nrd_pipeline(list("2019" = c(path, path)), identity),
    regexp = "single, non-empty parquet path string"
  )
  expect_error(
    nrd_pipeline(c("2019" = path), .f = 1),
    regexp = "`.f` must be a function"
  )
  expect_error(
    nrd_pipeline(c("2019" = path), identity, output_dir = c("a", "b")),
    regexp = "`output_dir` must be NULL or a single, non-empty directory path string"
  )
})

test_that("nrd_pipeline creates deterministic files in output_dir", {
  path_2018 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2018L,
    NRD_VisitLink = "A001",
    KEY_NRD = 101L,
    HOSP_NRD = 11L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L
  ))
  path_2019 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "B001",
    KEY_NRD = 201L,
    HOSP_NRD = 21L,
    DISCWT = 1,
    NRD_STRATUM = 201L,
    NRD_DaysToEvent = 8L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(c(path_2018, path_2019)), add = TRUE)

  out_dir <- tempfile("pipeline-out-")
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  pooled <- nrd_pipeline(
    c("2018" = path_2018, "2019" = path_2019),
    function(data) dplyr::filter(data, DIED == 0L),
    output_dir = out_dir
  )
  on.exit(nrd_close(pooled), add = TRUE)

  expect_true(file.exists(file.path(out_dir, "NRD_2018_pipeline.parquet")))
  expect_true(file.exists(file.path(out_dir, "NRD_2019_pipeline.parquet")))

  expect_true(nrd_close(pooled))
  expect_true(file.exists(file.path(out_dir, "NRD_2018_pipeline.parquet")))
  expect_true(file.exists(file.path(out_dir, "NRD_2019_pipeline.parquet")))
})

test_that("nrd_pipeline cleans temporary intermediate parquet directory on close", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  pooled <- nrd_pipeline(
    c("2019" = path),
    function(data) dplyr::filter(data, DIED == 0L)
  )

  owner <- .nrd_get_owner(pooled)
  pipeline_dir <- owner$cleanup_paths[[1]]

  expect_true(dir.exists(pipeline_dir))
  expect_true(file.exists(file.path(pipeline_dir, "NRD_2019_pipeline.parquet")))

  expect_true(nrd_close(pooled))
  expect_false(dir.exists(pipeline_dir))
})

test_that("nrd_pipeline cleans temporary intermediate parquet directory on error", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  before <- Sys.glob(file.path(tempdir(), "easynrd-pipeline-*"))

  expect_error(
    nrd_pipeline(
      c("2019" = path),
      function(data) rlang::abort("boom")
    ),
    regexp = "`nrd_pipeline\\(\\)` failed for year `2019`"
  )

  after <- Sys.glob(file.path(tempdir(), "easynrd-pipeline-*"))
  expect_setequal(after, before)
})

test_that("nrd_pipeline closes yearly resources when .f fails", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  close_calls <- 0L
  original_close <- nrd_close

  expect_error(
    testthat::with_mocked_bindings(
      nrd_pipeline(
        c("2019" = path),
        function(data) rlang::abort("boom")
      ),
      nrd_close = function(data) {
        close_calls <<- close_calls + 1L
        original_close(data)
      },
      .package = "easyNRD"
    ),
    regexp = "`nrd_pipeline\\(\\)` failed for year `2019`"
  )

  expect_equal(close_calls, 1L)
})

test_that("nrd_pipeline messages include each year label", {
  path_2018 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2018L,
    NRD_VisitLink = "A001",
    KEY_NRD = 101L,
    HOSP_NRD = 11L,
    DISCWT = 1,
    NRD_STRATUM = 101L,
    NRD_DaysToEvent = 5L,
    LOS = 2L,
    DMONTH = 1L,
    DIED = 0L
  ))
  path_2019 <- make_synthetic_nrd(dplyr::tibble(
    YEAR = 2019L,
    NRD_VisitLink = "B001",
    KEY_NRD = 201L,
    HOSP_NRD = 21L,
    DISCWT = 1,
    NRD_STRATUM = 201L,
    NRD_DaysToEvent = 8L,
    LOS = 3L,
    DMONTH = 1L,
    DIED = 0L
  ))
  on.exit(unlink(c(path_2018, path_2019)), add = TRUE)

  pooled <- NULL
  messages <- testthat::capture_messages(
    pooled <- nrd_pipeline(
      c("2018" = path_2018, "2019" = path_2019),
      function(data) dplyr::filter(data, DIED == 0L)
    )
  )
  on.exit(if (!is.null(pooled)) nrd_close(pooled), add = TRUE)

  expect_true(any(grepl("^\\[2018\\].*elapsed=", messages)))
  expect_true(any(grepl("^\\[2019\\].*elapsed=", messages)))
})
