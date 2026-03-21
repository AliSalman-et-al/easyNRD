#!/usr/bin/env Rscript

# easyNRD benchmark harness
#
# Usage:
#   Rscript data-raw/benchmarks/benchmark_pipeline.R
#
# What it does:
# - generates synthetic NRD parquet fixtures at 100K and 1M rows
# - benchmarks each lazy pipeline stage and the full end-to-end pipeline
# - profiles nrd_link_readmissions() on the 100K fixture with profvis
# - writes self-describing artifacts under data-raw/benchmarks/artifacts/
#
# This script is developer tooling only. It is not sourced by tests and is not
# executed during vignette builds or R CMD check.

required_packages <- c(
  "arrow",
  "bench",
  "DBI",
  "dbplyr",
  "dplyr",
  "duckdb",
  "htmlwidgets",
  "profvis",
  "tibble"
)

missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing_packages) > 0) {
  stop(
    "Missing required packages: ",
    paste(missing_packages, collapse = ", "),
    "\nInstall them before running this benchmark script.",
    call. = FALSE
  )
}

script_args <- commandArgs(trailingOnly = FALSE)
script_file_arg <- grep("^--file=", script_args, value = TRUE)
script_path <- if (length(script_file_arg) > 0) {
  normalizePath(sub("^--file=", "", script_file_arg[[1]]), winslash = "/", mustWork = TRUE)
} else {
  normalizePath("data-raw/benchmarks/benchmark_pipeline.R", winslash = "/", mustWork = TRUE)
}

repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."), winslash = "/", mustWork = TRUE)
setwd(repo_root)

r_files <- sort(list.files(file.path(repo_root, "R"), pattern = "\\.R$", full.names = TRUE))
invisible(lapply(r_files, source, local = globalenv()))

timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
artifact_root <- file.path(repo_root, "data-raw", "benchmarks", "artifacts")
artifact_dir <- file.path(artifact_root, paste0("benchmark_run_", timestamp))
fixture_dir <- file.path(artifact_dir, "fixtures")
profile_dir <- file.path(artifact_dir, "profiles")
results_dir <- file.path(artifact_dir, "results")
dir.create(fixture_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(profile_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(results_dir, recursive = TRUE, showWarnings = FALSE)

stage_iterations <- c(medium = 3L, large = 1L)
stage_order <- c(
  "ingest",
  "prepare",
  "flag_condition_regex",
  "flag_condition_codes",
  "link_readmissions",
  "select",
  "export",
  "end_to_end"
)

bytes_to_gb <- function(bytes) {
  round(bytes / 1024^3, 2)
}

safe_available_ram_bytes <- function() {
  if (.Platform$OS.type == "windows") {
    cmd <- "powershell -NoProfile -Command \"$os = Get-CimInstance Win32_OperatingSystem; [Console]::WriteLine([int64]$os.FreePhysicalMemory * 1024)\""
    value <- tryCatch(system(cmd, intern = TRUE), warning = function(w) NA_character_, error = function(e) NA_character_)
    value <- suppressWarnings(as.numeric(value[[1]]))
    if (!is.na(value)) {
      return(value)
    }
  }

  if (file.exists("/proc/meminfo")) {
    lines <- readLines("/proc/meminfo", warn = FALSE)
    match_line <- grep("^MemAvailable:", lines, value = TRUE)
    if (length(match_line) == 1) {
      value <- suppressWarnings(as.numeric(gsub("[^0-9]", "", match_line)))
      if (!is.na(value)) {
        return(value * 1024)
      }
    }
  }

  NA_real_
}

report_session_header <- function() {
  available_ram <- safe_available_ram_bytes()
  ram_label <- if (is.na(available_ram)) "unavailable" else paste0(bytes_to_gb(available_ram), " GiB")
  duckdb_version <- tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    DBI::dbGetQuery(con, "SELECT version() AS duckdb_version")$duckdb_version[[1]]
  }, error = function(e) as.character(utils::packageVersion("duckdb")))

  cat("Session summary:\n")
  cat(sprintf("- R version: %s\n", R.version.string))
  cat(sprintf("- DuckDB version: %s\n", duckdb_version))
  cat(sprintf("- Physical cores: %s\n", .nrd_available_physical_cores()))
  cat(sprintf("- Available RAM: %s\n", ram_label))
}

draw_patient_stay_sizes <- function(n_rows) {
  stay_sizes <- integer(0)
  stay_probs <- c(0.26, 0.24, 0.18, 0.12, 0.08, 0.06, 0.04, 0.02)

  while (sum(stay_sizes) < n_rows) {
    stay_sizes <- c(stay_sizes, sample.int(8L, size = 8192L, replace = TRUE, prob = stay_probs))
  }

  cumulative <- cumsum(stay_sizes)
  cutoff <- which(cumulative >= n_rows)[1]
  stay_sizes <- stay_sizes[seq_len(cutoff)]
  excess <- cumulative[[cutoff]] - n_rows
  if (excess > 0L) {
    stay_sizes[[cutoff]] <- stay_sizes[[cutoff]] - excess
  }

  stay_sizes[stay_sizes > 0L]
}

fixture_chunk_path <- function(scale_name, chunk_id) {
  file.path(fixture_dir, scale_name, sprintf("%s_fixture_chunk_%02d.parquet", scale_name, chunk_id))
}

make_dx_column <- function(n, column_index) {
  if (column_index <= 3L) {
    probs <- c(0.06, 0.04, 0.12, 0.10, 0.10, 0.08, 0.50)
  } else if (column_index <= 10L) {
    probs <- c(0.02, 0.02, 0.08, 0.08, 0.08, 0.06, 0.66)
  } else {
    probs <- c(0.005, 0.005, 0.03, 0.03, 0.03, 0.02, 0.88)
  }

  sample(
    c("I2101", "I214", "J189", "K359", "E119", "Z001", NA_character_),
    size = n,
    replace = TRUE,
    prob = probs
  )
}

make_pr_column <- function(n, column_index) {
  if (column_index <= 2L) {
    probs <- c(0.05, 0.10, 0.08, 0.07, 0.70)
  } else if (column_index <= 6L) {
    probs <- c(0.02, 0.08, 0.06, 0.04, 0.80)
  } else {
    probs <- c(0.005, 0.03, 0.03, 0.02, 0.915)
  }

  sample(
    c("02703ZZ", "0FT44ZZ", "5A1955Z", "0BH17EZ", NA_character_),
    size = n,
    replace = TRUE,
    prob = probs
  )
}

build_fixture_chunk <- function(patient_ids, stay_seq, key_start) {
  n <- length(patient_ids)
  unique_patients <- unique(patient_ids)
  patient_index <- match(patient_ids, unique_patients)
  base_day_by_patient <- sample.int(300L, length(unique_patients), replace = TRUE)
  step_by_patient <- sample(3L:55L, size = length(unique_patients), replace = TRUE, prob = exp(-(3L:55L) / 15))

  dte <- pmin.int(360L, base_day_by_patient[patient_index] + ((stay_seq - 1L) * step_by_patient[patient_index]))
  los <- sample(1L:7L, size = n, replace = TRUE, prob = c(0.20, 0.20, 0.18, 0.16, 0.12, 0.09, 0.05))

  out <- tibble::tibble(
    YEAR = 2019L,
    NRD_VISITLINK = sprintf("P%06d", patient_ids),
    KEY_NRD = key_start + seq_len(n) - 1L,
    HOSP_NRD = sample(1001L:1125L, size = n, replace = TRUE),
    DISCWT = round(stats::runif(n, min = 0.2, max = 4.5), 3),
    NRD_STRATUM = sample(1L:30L, size = n, replace = TRUE),
    NRD_DaysToEvent = dte,
    LOS = los,
    DMONTH = pmin.int(12L, ((dte - 1L) %/% 30L) + 1L),
    DIED = sample(c(0L, 1L), size = n, replace = TRUE, prob = c(0.985, 0.015)),
    FEMALE = sample(c(0L, 1L), size = n, replace = TRUE)
  )

  for (dx_idx in seq_len(40L)) {
    out[[sprintf("I10_DX%d", dx_idx)]] <- make_dx_column(n, dx_idx)
  }

  for (pr_idx in seq_len(25L)) {
    out[[sprintf("I10_PR%d", pr_idx)]] <- make_pr_column(n, pr_idx)
  }

  out
}

generate_fixture_paths <- function(scale_name, n_rows, chunk_rows) {
  scale_dir <- file.path(fixture_dir, scale_name)
  dir.create(scale_dir, recursive = TRUE, showWarnings = FALSE)

  stay_sizes <- draw_patient_stay_sizes(n_rows)
  patient_ids <- rep.int(seq_along(stay_sizes), stay_sizes)
  stay_seq <- sequence(stay_sizes)
  n_patients <- length(stay_sizes)

  chunk_ids <- ceiling(cumsum(stay_sizes) / chunk_rows)
  split_patients <- split(seq_len(n_patients), chunk_ids)
  key_start <- 1000000L
  paths <- character(length(split_patients))

  for (i in seq_along(split_patients)) {
    patient_chunk <- split_patients[[i]]
    idx <- patient_ids %in% patient_chunk
    chunk <- build_fixture_chunk(patient_ids = patient_ids[idx], stay_seq = stay_seq[idx], key_start = key_start)
    key_start <- key_start + nrow(chunk)
    paths[[i]] <- fixture_chunk_path(scale_name, i)
    arrow::write_parquet(chunk, sink = paths[[i]])
  }

  list(
    paths = paths,
    rows = n_rows,
    patients = n_patients,
    mean_stays_per_patient = round(n_rows / n_patients, 2),
    max_stays_per_patient = max(stay_sizes)
  )
}

get_temp_dir <- function(data) {
  con <- dbplyr::remote_con(data)
  DBI::dbGetQuery(con, "SELECT current_setting('temp_directory') AS temp_directory")$temp_directory[[1]]
}

directory_size_bytes <- function(path) {
  if (!dir.exists(path)) {
    return(0)
  }

  files <- list.files(path, recursive = TRUE, full.names = TRUE, all.files = TRUE, no.. = TRUE)
  file_paths <- files[file.exists(files)]
  if (length(file_paths) == 0) {
    return(0)
  }

  sum(file.info(file_paths)$size, na.rm = TRUE)
}

force_lazy_stage <- function(data) {
  data |>
    dplyr::summarise(rows = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(rows)
}

build_analysis_pipeline <- function(paths) {
  nrd_ingest(paths) |>
    nrd_prepare() |>
    nrd_flag_condition("is_ami", pattern = "^I21", type = "dx", scope = "all") |>
    nrd_flag_condition("has_revascularization", codes = c("02703ZZ"), type = "pr", scope = "all")
}

run_ingest_stage <- function(paths) {
  data <- nrd_ingest(paths)
  on.exit(nrd_close(data), add = TRUE)

  temp_dir <- get_temp_dir(data)
  before_bytes <- directory_size_bytes(temp_dir)
  rows <- force_lazy_stage(data)
  after_bytes <- directory_size_bytes(temp_dir)

  list(rows = rows, spilled = after_bytes > before_bytes, spill_bytes = after_bytes - before_bytes)
}

run_lazy_stage <- function(paths, builder) {
  data <- builder(paths)
  on.exit(nrd_close(data), add = TRUE)

  temp_dir <- get_temp_dir(data)
  before_bytes <- directory_size_bytes(temp_dir)
  rows <- force_lazy_stage(data)
  after_bytes <- directory_size_bytes(temp_dir)

  list(rows = rows, spilled = after_bytes > before_bytes, spill_bytes = after_bytes - before_bytes)
}

run_export_stage <- function(paths, export_name) {
  data <- build_analysis_pipeline(paths) |>
    nrd_link_readmissions(
      index_condition = is_ami,
      readmit_condition = has_revascularization,
      window = 30L,
      readmit_vars = c(HOSP_NRD, FEMALE)
    ) |>
    nrd_select(HOSP_NRD, FEMALE, is_ami, has_revascularization, readmit_HOSP_NRD, readmit_FEMALE)
  on.exit(nrd_close(data), add = TRUE)

  temp_dir <- get_temp_dir(data)
  before_bytes <- directory_size_bytes(temp_dir)
  out_path <- file.path(results_dir, sprintf("%s_output.parquet", export_name))
  nrd_export(data, out_path)
  rows <- arrow::open_dataset(out_path, format = "parquet") |>
    dplyr::summarise(rows = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(rows)
  after_bytes <- directory_size_bytes(temp_dir)

  list(rows = rows, spilled = after_bytes > before_bytes, spill_bytes = after_bytes - before_bytes, export_path = out_path)
}

stage_measurement <- function(stage_name, paths, scale_name, iterations) {
  run_once <- switch(
    stage_name,
    ingest = function() run_ingest_stage(paths),
    prepare = function() run_lazy_stage(paths, function(x) nrd_ingest(x) |> nrd_prepare()),
    flag_condition_regex = function() run_lazy_stage(paths, function(x) nrd_ingest(x) |> nrd_flag_condition("is_ami", pattern = "^I21", type = "dx", scope = "all")),
    flag_condition_codes = function() run_lazy_stage(paths, function(x) nrd_ingest(x) |> nrd_flag_condition("has_revascularization", codes = c("02703ZZ"), type = "pr", scope = "all")),
    link_readmissions = function() run_lazy_stage(paths, function(x) {
      build_analysis_pipeline(x) |>
        nrd_link_readmissions(
          index_condition = is_ami,
          readmit_condition = has_revascularization,
          window = 30L,
          readmit_vars = c(HOSP_NRD, FEMALE)
        )
    }),
    select = function() run_lazy_stage(paths, function(x) {
      build_analysis_pipeline(x) |>
        nrd_link_readmissions(
          index_condition = is_ami,
          readmit_condition = has_revascularization,
          window = 30L,
          readmit_vars = c(HOSP_NRD, FEMALE)
        ) |>
        nrd_select(HOSP_NRD, FEMALE, is_ami, has_revascularization, readmit_HOSP_NRD, readmit_FEMALE)
    }),
    export = function() run_export_stage(paths, sprintf("%s_export", scale_name)),
    end_to_end = function() run_export_stage(paths, sprintf("%s_end_to_end", scale_name))
  )

  wall_timer <- system.time(run_result <- run_once())[["elapsed"]]
  bench_result <- bench::mark(
    run_once(),
    iterations = iterations,
    check = FALSE,
    memory = TRUE,
    filter_gc = FALSE
  )

  tibble::tibble(
    scale = scale_name,
    stage = stage_name,
    iterations = iterations,
    rows = run_result$rows,
    wall_seconds = unname(wall_timer),
    min_seconds = as.numeric(bench_result$min, units = "s"),
    median_seconds = as.numeric(bench_result$median, units = "s"),
    mem_alloc_bytes = as.numeric(bench_result$mem_alloc),
    gc_count = sum(bench_result$n_gc),
    spilled = run_result$spilled,
    spill_bytes = run_result$spill_bytes
  )
}

profile_link_readmissions <- function(paths) {
  profile <- profvis::profvis({
    data <- build_analysis_pipeline(paths)
    on.exit(nrd_close(data), add = TRUE)

    data |>
      nrd_link_readmissions(
        index_condition = is_ami,
        readmit_condition = has_revascularization,
        window = 30L,
        readmit_vars = c(HOSP_NRD, FEMALE)
      ) |>
      force_lazy_stage()
  }, interval = 0.01)

  out_path <- file.path(profile_dir, "profvis_link_readmissions_medium.html")
  htmlwidgets::saveWidget(profile, file = out_path, selfcontained = TRUE)
  out_path
}

write_manifest <- function(fixture_summary, benchmark_table, profvis_path) {
  manifest_path <- file.path(results_dir, "benchmark_manifest.txt")
  lines <- c(
    sprintf("run_id: benchmark_run_%s", timestamp),
    sprintf("created_at: %s", format(Sys.time(), tz = "UTC", usetz = TRUE)),
    sprintf("script: %s", normalizePath(script_path, winslash = "/", mustWork = TRUE)),
    sprintf("profvis_html: %s", normalizePath(profvis_path, winslash = "/", mustWork = TRUE)),
    sprintf("benchmark_csv: %s", normalizePath(file.path(results_dir, "benchmark_summary.csv"), winslash = "/", mustWork = TRUE)),
    sprintf("benchmark_rds: %s", normalizePath(file.path(results_dir, "benchmark_summary.rds"), winslash = "/", mustWork = TRUE)),
    "",
    "fixture_summary:",
    paste(capture.output(print(fixture_summary)), collapse = "\n"),
    "",
    "benchmark_summary:",
    paste(capture.output(print(benchmark_table)), collapse = "\n")
  )
  writeLines(lines, con = manifest_path)
}

report_session_header()
cat("\nGenerating synthetic benchmark fixtures...\n")

fixtures <- list(
  medium = generate_fixture_paths("medium", n_rows = 100000L, chunk_rows = 20000L),
  large = generate_fixture_paths("large", n_rows = 1000000L, chunk_rows = 100000L)
)

fixture_summary <- tibble::tibble(
  scale = names(fixtures),
  rows = vapply(fixtures, `[[`, integer(1), "rows"),
  patients = vapply(fixtures, `[[`, integer(1), "patients"),
  mean_stays_per_patient = vapply(fixtures, `[[`, numeric(1), "mean_stays_per_patient"),
  max_stays_per_patient = vapply(fixtures, `[[`, integer(1), "max_stays_per_patient")
)

cat("Fixture summary:\n")
print(fixture_summary)

benchmark_rows <- list()
for (scale_name in names(fixtures)) {
  cat(sprintf("\nRunning benchmarks for %s fixture...\n", scale_name))
  for (stage_name in stage_order) {
    cat(sprintf("- %s\n", stage_name))
    benchmark_rows[[length(benchmark_rows) + 1L]] <- stage_measurement(
      stage_name = stage_name,
      paths = fixtures[[scale_name]]$paths,
      scale_name = scale_name,
      iterations = unname(stage_iterations[[scale_name]])
    )
  }
}

benchmark_table <- dplyr::bind_rows(benchmark_rows) |>
  dplyr::arrange(factor(scale, levels = c("medium", "large")), factor(stage, levels = stage_order))

csv_path <- file.path(results_dir, "benchmark_summary.csv")
rds_path <- file.path(results_dir, "benchmark_summary.rds")
utils::write.csv(benchmark_table, file = csv_path, row.names = FALSE)
saveRDS(benchmark_table, file = rds_path)

cat("\nProfiling nrd_link_readmissions() on medium fixture...\n")
profvis_path <- profile_link_readmissions(fixtures$medium$paths)

console_summary <- benchmark_table |>
  dplyr::mutate(
    wall_seconds = round(wall_seconds, 3),
    min_seconds = round(min_seconds, 3),
    median_seconds = round(median_seconds, 3),
    mem_alloc_mb = round(mem_alloc_bytes / 1024^2, 2),
    spill_mb = round(spill_bytes / 1024^2, 2)
  ) |>
  dplyr::select(
    scale,
    stage,
    iterations,
    rows,
    wall_seconds,
    min_seconds,
    median_seconds,
    mem_alloc_mb,
    gc_count,
    spilled,
    spill_mb
  )

write_manifest(fixture_summary, console_summary, profvis_path)

cat("\nBenchmark summary:\n")
print(console_summary, n = nrow(console_summary))

cat(sprintf("\nResults CSV: %s\n", normalizePath(csv_path, winslash = "/", mustWork = TRUE)))
cat(sprintf("Results RDS: %s\n", normalizePath(rds_path, winslash = "/", mustWork = TRUE)))
cat(sprintf("Profile HTML: %s\n", normalizePath(profvis_path, winslash = "/", mustWork = TRUE)))
cat(sprintf("Manifest: %s\n", normalizePath(file.path(results_dir, "benchmark_manifest.txt"), winslash = "/", mustWork = TRUE)))
