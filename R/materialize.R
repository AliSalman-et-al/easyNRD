#' Materialize a Lazy NRD Pipeline Checkpoint
#'
#' `nrd_materialize()` forces evaluation of a lazy NRD pipeline and returns a
#' new lazy DuckDB table that can be chained into downstream steps. Use this as
#' a checkpoint after regex-heavy phenotyping across many diagnosis fields, and
#' before episode construction or readmission linkage.
#'
#' @param data A DuckDB-backed lazy table produced by `easyNRD` pipeline steps.
#' @param name Table name used when materializing in-database with
#'   [dplyr::compute()]. Defaults to `"m_cohort"`.
#' @param path Optional parquet output path. If `NULL` (default), materializes
#'   to a DuckDB table with [dplyr::compute()]. If provided, exports to parquet
#'   and re-ingests the file into a fresh lazy DuckDB table.
#' @param row_group_size Target parquet row-group size used when `path` is
#'   provided. Smaller groups can reduce peak memory pressure for wide tables.
#'   Defaults to `100000L`.
#'
#' @returns A DuckDB-backed lazy table (`tbl_lazy`) pointing to the materialized
#'   checkpoint, suitable for direct piping to [nrd_link_readmissions()] or
#'   [nrd_as_survey()].
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   checkpoint <- nrd_ingest("/path/to/nrd.parquet") |>
#'     dplyr::mutate(ami_flag = stringr::str_detect(Episode_DX10, "I21")) |>
#'     nrd_materialize(name = "m_ami")
#'
#'   checkpoint_disk <- checkpoint |>
#'     nrd_materialize(path = "/path/to/checkpoints/ami.parquet")
#' }
#' }
nrd_materialize <- function(
  data,
  name = "m_cohort",
  path = NULL,
  row_group_size = 100000L
) {
  .nrd_assert_lazy_duckdb(data, arg = "data")

  con <- tryCatch(dbplyr::remote_con(data), error = function(e) NULL)
  if (is.null(con) || !DBI::dbIsValid(con)) {
    cli::cli_abort(c(
      "No active DuckDB connection found for `data`.",
      i = "Recreate the pipeline with `nrd_ingest()` and retry materialization."
    ))
  }

  if (!is.character(name) || length(name) != 1 || is.na(name) || nchar(name) == 0) {
    cli::cli_abort(c(
      "`name` must be a single, non-empty table name string.",
      i = "Example: `name = \"m_cohort\"`."
    ))
  }

  if (!is.numeric(row_group_size) ||
    length(row_group_size) != 1 ||
    is.na(row_group_size) ||
    row_group_size <= 0) {
    cli::cli_abort(c(
      "`row_group_size` must be a single positive numeric or integer value.",
      i = "Use smaller values for wide clinical feature tables."
    ))
  }
  row_group_size <- as.integer(row_group_size)

  if (is.null(path)) {
    return(dplyr::compute(data, name = name))
  }

  if (!is.character(path) || length(path) != 1 || is.na(path) || nchar(path) == 0) {
    cli::cli_abort(c(
      "`path` must be a single, non-empty file path string.",
      i = "Provide a `.parquet` file path for on-disk checkpoints."
    ))
  }

  if (dir.exists(path)) {
    cli::cli_abort(c(
      "`path` points to an existing directory, not a parquet file.",
      i = "Use a full file path ending in `.parquet`."
    ))
  }

  if (!grepl("\\.parquet$", path, ignore.case = TRUE)) {
    cli::cli_abort(c(
      "`path` must end in `.parquet`.",
      i = "Example: `/scratch/checkpoints/m_cohort.parquet`."
    ))
  }

  nrd_export(data, path = path, row_group_size = row_group_size)
  nrd_ingest(path)
}
