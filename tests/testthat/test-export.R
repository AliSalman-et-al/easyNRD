test_that("lazy DuckDB table exports to a readable parquet file", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- tempfile(fileext = ".parquet")
  on.exit(unlink(out), add = TRUE)

  expect_invisible(nrd_export(data, out))
  expect_true(file.exists(out))

  exported <- arrow::read_parquet(out)
  expect_equal(nrow(exported), 4L)
  expect_true("NRD_VISITLINK" %in% names(exported))
})

test_that("nrd_export creates parent directories when needed", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out_dir <- file.path(tempdir(), paste0("export-parent-", as.integer(stats::runif(1, 1, 1e6))))
  out <- file.path(out_dir, "nested", "nrd.parquet")
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  nrd_export(data, out)

  expect_true(file.exists(out))
})

test_that("non-parquet export path produces a clear error", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  expect_error(
    nrd_export(data, tempfile(fileext = ".csv")),
    "\\.parquet"
  )
})

test_that("export uses ZSTD compression and row group size 122880", {
  path <- make_synthetic_nrd()
  on.exit(unlink(path), add = TRUE)

  data <- nrd_ingest(path)
  on.exit(nrd_close(data), add = TRUE)

  out <- tempfile(fileext = ".parquet")
  on.exit(unlink(out), add = TRUE)

  nrd_export(data, out)

  meta_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(meta_con, shutdown = TRUE), add = TRUE)

  metadata <- DBI::dbGetQuery(
    meta_con,
    paste0(
      "SELECT row_group_id, row_group_num_rows, compression ",
      "FROM parquet_metadata(",
      as.character(DBI::dbQuoteString(meta_con, normalizePath(out, winslash = "/", mustWork = TRUE))),
      ")"
    )
  )

  expect_true(nrow(metadata) >= 1L)
  expect_true(all(toupper(metadata$compression) == "ZSTD"))
  expect_true(all(metadata$row_group_num_rows <= 122880L))
})
