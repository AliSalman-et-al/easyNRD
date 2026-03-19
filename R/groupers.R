.nrd_apply_grouper <- function(data, xwalk_data) {
  .nrd_assert_lazy_duckdb(data, arg = "data")

  con <- dbplyr::remote_con(data)
  long_dx <- nrd_pivot_clinical(data, type = "diagnoses")
  xwalk_tbl <- dbplyr::copy_inline(con, xwalk_data)

  flag_cols <- setdiff(names(xwalk_data), "ICD10_Code")
  if (length(flag_cols) == 0) {
    rlang::abort("Crosswalk table must contain at least one condition flag column.")
  }

  grouped_flags <- long_dx |>
    dplyr::inner_join(xwalk_tbl, by = "ICD10_Code") |>
    dplyr::group_by(KEY_NRD) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(flag_cols),
        ~ max(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    )

  dplyr::left_join(data, grouped_flags, by = "KEY_NRD")
}

#' Add Elixhauser Comorbidity Flags
#'
#' `nrd_add_elixhauser()` applies a diagnosis-code crosswalk to produce
#' hospitalization-level binary Elixhauser indicators.
#'
#' @param data A lazy DuckDB table containing diagnosis columns.
#'
#' @return A lazy DuckDB table with Elixhauser indicator columns appended.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   episodes <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes()
#'
#'   episodes_elix <- nrd_add_elixhauser(episodes)
#' }
nrd_add_elixhauser <- function(data) {
  .nrd_apply_grouper(data, nrd_elixhauser_xwalk)
}

#' Add CCSR Condition Category Flags
#'
#' `nrd_add_ccsr()` applies a diagnosis-code crosswalk to produce
#' hospitalization-level binary CCSR indicators.
#'
#' @param data A lazy DuckDB table containing diagnosis columns.
#'
#' @return A lazy DuckDB table with CCSR indicator columns appended.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   episodes <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes()
#'
#'   episodes_ccsr <- nrd_add_ccsr(episodes)
#' }
nrd_add_ccsr <- function(data) {
  .nrd_apply_grouper(data, nrd_ccsr_xwalk)
}
