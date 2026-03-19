#' Flag Clinical Conditions via Dictionary Filtering
#'
#' `nrd_flag_condition()` builds an exact-match ICD-10 dictionary by evaluating
#' a regular expression against the distinct set of clinical codes in the source
#' table. The resulting dictionary is injected into `dplyr::if_any()` so
#' `dbplyr` can render SQL `IN (...)` predicates instead of query-time regex
#' matching across concatenated strings.
#'
#' @param data A lazy DuckDB table.
#' @param condition_name Name of the output binary flag column.
#' @param regex_pattern A regular expression used in-memory to define the code
#'   dictionary.
#' @param type Clinical code family. Use `"dx"` for diagnosis fields or `"pr"`
#'   for procedure fields.
#' @param scope Column scope for matching. Use `"principal"`, `"secondary"`, or
#'   `"all"`.
#'
#' @return A lazy DuckDB table with one additional logical condition flag column.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   flagged <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_flag_condition(
#'       condition_name = "is_ami",
#'       regex_pattern = "^I21",
#'       type = "dx",
#'       scope = "principal"
#'     )
#' }
nrd_flag_condition <- function(
  data,
  condition_name,
  regex_pattern,
  type = c("dx", "pr"),
  scope = c("all", "principal", "secondary")
) {
  .nrd_assert_lazy_duckdb(data, arg = "data")

  if (!is.character(condition_name) || length(condition_name) != 1 || is.na(condition_name) || !nzchar(condition_name)) {
    rlang::abort("`condition_name` must be a single non-empty string.")
  }

  if (!is.character(regex_pattern) || length(regex_pattern) != 1 || is.na(regex_pattern) || !nzchar(regex_pattern)) {
    rlang::abort("`regex_pattern` must be a single non-empty string.")
  }

  type <- rlang::arg_match(type)
  scope <- rlang::arg_match(scope)

  selector_expr <- if (identical(type, "dx") && identical(scope, "principal")) {
    rlang::expr(I10_DX1)
  } else if (identical(type, "dx") && identical(scope, "secondary")) {
    rlang::expr(tidyselect::num_range("I10_DX", 2:40))
  } else if (identical(type, "dx") && identical(scope, "all")) {
    rlang::expr(tidyselect::starts_with("I10_DX"))
  } else if (identical(type, "pr") && identical(scope, "principal")) {
    rlang::expr(I10_PR1)
  } else if (identical(type, "pr") && identical(scope, "secondary")) {
    rlang::expr(tidyselect::num_range("I10_PR", 2:25))
  } else {
    rlang::expr(tidyselect::starts_with("I10_PR"))
  }

  scoped_cols <- names(tidyselect::eval_select(selector_expr, data = data))

  if (length(scoped_cols) == 0) {
    rlang::abort("No clinical columns matched `type` and `scope`.")
  }

  distinct_codes <- data |>
    dplyr::select(dplyr::all_of(scoped_cols)) |>
    tidyr::pivot_longer(
      cols = dplyr::everything(),
      names_to = "Code_Position",
      values_to = "ICD10_Code"
    ) |>
    dplyr::filter(!is.na(ICD10_Code)) |>
    dplyr::distinct(ICD10_Code) |>
    dplyr::collect() |>
    dplyr::pull(ICD10_Code)

  exact_dictionary <- distinct_codes[grepl(regex_pattern, distinct_codes, perl = TRUE)]
  exact_dictionary <- unique(stats::na.omit(exact_dictionary))

  condition_sym <- rlang::sym(condition_name)

  if (length(exact_dictionary) == 0) {
    return(dplyr::mutate(data, !!condition_sym := FALSE))
  }

  dplyr::mutate(
    data,
    !!condition_sym := dplyr::coalesce(
      dplyr::if_any(
        dplyr::all_of(scoped_cols),
        ~ .x %in% !!exact_dictionary
      ),
      FALSE
    )
  )
}
