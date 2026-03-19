test_that("nrd_configure_engine validates connection and temp directory", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(
    nrd_configure_engine(
      con,
      memory_limit = "512MB",
      temp_directory = tempdir(),
      threads = 1L,
      preserve_insertion_order = FALSE
    )
  )

  preserve_setting <- DBI::dbGetQuery(
    con,
    "SELECT current_setting('preserve_insertion_order') AS preserve_insertion_order"
  )$preserve_insertion_order[[1]]
  expect_identical(toupper(as.character(preserve_setting)), "FALSE")

  expect_error(
    nrd_configure_engine(con, temp_directory = tempfile("missing_dir_")),
    "must exist"
  )
})

test_that("nrd_ingest configures per-process spill directory", {
  temp_root <- tempfile("nrd_ingest_tmp_")
  dir.create(temp_root, recursive = TRUE)

  parquet_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(
    tibble::tibble(
      YEAR = 2019L,
      NRD_VISITLINK = "A001",
      KEY_NRD = 1L,
      NRD_DAYSTOEVENT = 5L,
      LOS = 2L,
      DISPUNIFORM = 1L,
      DMONTH = 1L,
      DIED = 0L,
      I10_DX1 = "E119"
    ),
    parquet_path
  )

  lazy_tbl <- nrd_ingest(
    parquet_path,
    temp_directory = temp_root,
    memory_limit = "256MB",
    threads = 1L,
    preserve_insertion_order = FALSE
  )

  nrd_env <- attr(lazy_tbl, "nrd_env", exact = TRUE)
  expect_true(is.environment(nrd_env))
  expect_true(dir.exists(nrd_env$session_temp_dir))
  expect_true(startsWith(normalizePath(nrd_env$session_temp_dir), normalizePath(temp_root)))
  expect_match(basename(nrd_env$session_temp_dir), "^easyNRD_[0-9]+$")

  temp_setting <- DBI::dbGetQuery(
    nrd_env$con,
    "SELECT current_setting('temp_directory') AS temp_directory"
  )$temp_directory[[1]]
  expect_identical(normalizePath(temp_setting), normalizePath(nrd_env$session_temp_dir))

  if (DBI::dbIsValid(nrd_env$con)) {
    DBI::dbDisconnect(nrd_env$con, shutdown = TRUE)
  }
})

test_that("nrd_flag_condition renders SQL IN clauses without regex or concat", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    KEY_NRD = 1:3,
    NRD_VISITLINK = c("A", "B", "C"),
    HOSP_NRD = c(10L, 10L, 20L),
    I10_DX1 = c("E119", "I10", "Z001"),
    I10_DX2 = c("N179", NA, "R509"),
    I10_PR1 = c("0JH60DZ", "30233N1", NA)
  )

  DBI::dbWriteTable(con, "phenotype_input", dat)
  lazy_tbl <- dplyr::tbl(con, "phenotype_input")

  flagged <- nrd_flag_condition(
    lazy_tbl,
    condition_name = "is_diabetes",
    regex_pattern = "^E11",
    type = "dx",
    scope = "all"
  )

  sql <- dbplyr::sql_render(flagged)

  expect_match(sql, "\\bIN\\s*\\(", perl = TRUE)
  expect_no_match(sql, "REGEXP|regexp_matches", ignore.case = TRUE)
  expect_no_match(sql, "CONCAT_WS|\\|\\|", perl = TRUE, ignore.case = TRUE)

  out <- dplyr::collect(flagged)
  expect_identical(out$is_diabetes, c(TRUE, FALSE, FALSE))
})

test_that("nrd_pivot_clinical preserves identifiers and drops NA codes", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    NRD_VISITLINK = c("A", "B"),
    KEY_NRD = c(1001L, 1002L),
    HOSP_NRD = c(11L, 12L),
    I10_DX1 = c("E119", "I10"),
    I10_DX2 = c(NA, "N179"),
    I10_PR1 = c("0JH60DZ", NA)
  )

  DBI::dbWriteTable(con, "pivot_input", dat)
  lazy_tbl <- dplyr::tbl(con, "pivot_input")

  long_dx <- nrd_pivot_clinical(lazy_tbl, type = "diagnoses")
  out_dx <- dplyr::collect(long_dx)

  expect_true(all(c("NRD_VISITLINK", "KEY_NRD", "HOSP_NRD", "Code_Position", "ICD10_Code") %in% names(out_dx)))
  expect_false(any(is.na(out_dx$ICD10_Code)))
  expect_equal(nrow(out_dx), 3)
})

test_that("nrd_add_elixhauser and nrd_add_ccsr append hospitalization flags", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dat <- tibble::tibble(
    NRD_VISITLINK = c("A", "B", "C"),
    KEY_NRD = c(2001L, 2002L, 2003L),
    HOSP_NRD = c(1L, 1L, 2L),
    I10_DX1 = c("E119", "I10", "Z001"),
    I10_DX2 = c(NA, "N179", NA)
  )

  DBI::dbWriteTable(con, "grouper_input", dat)
  lazy_tbl <- dplyr::tbl(con, "grouper_input")

  elix <- dplyr::collect(nrd_add_elixhauser(lazy_tbl))
  ccsr <- dplyr::collect(nrd_add_ccsr(lazy_tbl))

  expect_equal(elix$Uncomplicated_Diabetes[elix$KEY_NRD == 2001L], 1)
  expect_equal(elix$Hypertension[elix$KEY_NRD == 2002L], 1)
  expect_equal(elix$Renal_Failure[elix$KEY_NRD == 2002L], 1)
  expect_true(is.na(elix$Hypertension[elix$KEY_NRD == 2003L]))

  expect_equal(ccsr$END002[ccsr$KEY_NRD == 2001L], 1)
  expect_equal(ccsr$CIR007[ccsr$KEY_NRD == 2002L], 1)
  expect_equal(ccsr$GEN003[ccsr$KEY_NRD == 2002L], 1)
  expect_true(is.na(ccsr$CIR007[ccsr$KEY_NRD == 2003L]))
})
