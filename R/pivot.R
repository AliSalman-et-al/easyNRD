#' Pivot Wide Clinical Columns to Long Event Format
#'
#' `nrd_pivot_clinical()` reshapes wide diagnosis or procedure arrays into a
#' long clinical event table while preserving NRD hospitalization identifiers.
#'
#' @param data A lazy DuckDB table.
#' @param type Clinical code family to pivot: `"diagnoses"` or
#'   `"procedures"`.
#'
#' @return A lazy long-format table with `NRD_VISITLINK`, `KEY_NRD`,
#'   `HOSP_NRD`, `Code_Position`, and `ICD10_Code`.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   dx_long <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_pivot_clinical(type = "diagnoses")
#' }
nrd_pivot_clinical <- function(data, type = c("diagnoses", "procedures")) {
  .nrd_assert_lazy_duckdb(data, arg = "data")
  type <- rlang::arg_match(type)

  .nrd_assert_cols(data, c("NRD_VISITLINK", "KEY_NRD", "HOSP_NRD"))

  code_selector <- if (identical(type, "diagnoses")) {
    rlang::expr(tidyselect::num_range("I10_DX", 1:40))
  } else {
    rlang::expr(tidyselect::num_range("I10_PR", 1:25))
  }

  code_cols <- names(tidyselect::eval_select(code_selector, data = data))

  if (length(code_cols) == 0) {
    rlang::abort("No clinical columns found for the requested `type`.")
  }

  data |>
    dplyr::select(NRD_VISITLINK, KEY_NRD, HOSP_NRD, dplyr::all_of(code_cols)) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(code_cols),
      names_to = "Code_Position",
      values_to = "ICD10_Code"
    ) |>
    dplyr::filter(!is.na(ICD10_Code))
}
