test_that("nrd_configure_engine validates connection and temp directory", {
  with_duckdb_connection(function(con) {
    tmp_root <- tempfile("nrd_cfg_tmp_")
    dir.create(tmp_root, recursive = TRUE)
    on.exit(unlink(tmp_root, recursive = TRUE, force = TRUE), add = TRUE)

    expect_no_error(
      nrd_configure_engine(
        con,
        memory_limit = "512MB",
        temp_directory = tmp_root,
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
})

test_that("nrd_ingest configures per-process spill directory", {
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

  lazy_tbl <- nrd_ingest(parquet_path)

  info <- nrd_connection_info(lazy_tbl)
  expect_true(info$managed)
  expect_false(info$closed)
  expect_true(dir.exists(info$temp_directory))
  expect_true(startsWith(normalizePath(info$temp_directory), normalizePath(nrd_cache_dir())))
  expect_match(basename(info$temp_directory), "^easyNRD_[0-9]+(?:_.+)?$", perl = TRUE)

  temp_setting <- DBI::dbGetQuery(
    dbplyr::remote_con(lazy_tbl),
    "SELECT current_setting('temp_directory') AS temp_directory"
  )$temp_directory[[1]]
  expect_identical(normalizePath(temp_setting), normalizePath(info$temp_directory))
  session_temp_dir <- info$temp_directory

  expect_true(nrd_close(lazy_tbl))
  expect_false(dir.exists(session_temp_dir))
})

test_that("nrd_ingest uses unique spill directories and closes idempotently", {
  parquet_path_a <- tempfile(fileext = ".parquet")
  parquet_path_b <- tempfile(fileext = ".parquet")

  dat <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = "A001",
    KEY_NRD = 1L,
    NRD_DAYSTOEVENT = 5L,
    LOS = 2L,
    DISPUNIFORM = 1L,
    DMONTH = 1L,
    DIED = 0L,
    I10_DX1 = "E119"
  )

  arrow::write_parquet(dat, parquet_path_a)
  arrow::write_parquet(dat, parquet_path_b)

  lazy_tbl_a <- nrd_ingest(parquet_path_a)
  lazy_tbl_b <- nrd_ingest(parquet_path_b)

  info_a <- nrd_connection_info(lazy_tbl_a)
  info_b <- nrd_connection_info(lazy_tbl_b)
  session_temp_dir_a <- info_a$temp_directory
  session_temp_dir_b <- info_b$temp_directory

  expect_false(identical(normalizePath(session_temp_dir_a), normalizePath(session_temp_dir_b)))

  expect_true(nrd_close(lazy_tbl_a))
  expect_false(dir.exists(session_temp_dir_a))

  expect_false(nrd_close(lazy_tbl_a))

  expect_true(nrd_close(lazy_tbl_b))
  expect_false(dir.exists(session_temp_dir_b))
})

test_that("nrd_flag_condition renders SQL IN clauses without regex or concat", {
  with_duckdb_connection(function(con) {
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
      regex_pattern = "^(E11|S82)",
      type = "dx",
      scope = "all"
    )

    sql <- dbplyr::sql_render(flagged)

    expect_match(sql, "\\bIN\\s*\\(", perl = TRUE)
    expect_match(sql, "S82001A", fixed = TRUE)
    expect_no_match(sql, "REGEXP|regexp_matches", ignore.case = TRUE)
    expect_no_match(sql, "CONCAT_WS|\\|\\|", perl = TRUE, ignore.case = TRUE)

    out <- flagged |>
      dplyr::arrange(KEY_NRD) |>
      dplyr::collect()
    expect_identical(out$is_diabetes, c(TRUE, FALSE, FALSE))
  })
})

test_that("nrd_pivot_clinical preserves identifiers and drops NA codes", {
  with_duckdb_connection(function(con) {
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
})

test_that("nrd_add_elixhauser and nrd_add_ccsr append hospitalization flags", {
  with_duckdb_connection(function(con) {
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
})
