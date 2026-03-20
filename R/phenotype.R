# Resolve diagnosis or procedure columns for a requested scope.
.nrd_condition_columns <- function(data, type, scope) {
  selector <- if (identical(type, "dx") && identical(scope, "principal")) {
    rlang::expr(dplyr::all_of("I10_DX1"))
  } else if (identical(type, "dx") && identical(scope, "secondary")) {
    rlang::expr(tidyselect::num_range("I10_DX", 2:40))
  } else if (identical(type, "dx")) {
    rlang::expr(tidyselect::starts_with("I10_DX"))
  } else if (identical(type, "pr") && identical(scope, "principal")) {
    rlang::expr(dplyr::all_of("I10_PR1"))
  } else if (identical(type, "pr") && identical(scope, "secondary")) {
    rlang::expr(tidyselect::num_range("I10_PR", 2:25))
  } else {
    rlang::expr(tidyselect::starts_with("I10_PR"))
  }

  cols <- names(tidyselect::eval_select(selector, data = data))
  if (length(cols) == 0) {
    rlang::abort(
      paste0(
        "No columns matched `type = \"", type, "\"` and `scope = \"", scope, "\"`."
      )
    )
  }

  cols
}

# Build the condition expression for regex or exact-code matching.
.nrd_condition_predicate <- function(cols, pattern = NULL, codes = NULL) {
  if (!is.null(pattern)) {
    return(
      rlang::expr(
        dplyr::if_any(
          dplyr::all_of(!!cols),
          ~ grepl(!!pattern, .x)
        )
      )
    )
  }

  rlang::expr(
    dplyr::if_any(
      dplyr::all_of(!!cols),
      ~ .x %in% !!codes
    )
  )
}

#' Flag diagnosis or procedure conditions
#'
#' `nrd_flag_condition()` adds a logical flag based on regex matching or exact
#' code matching across NRD diagnosis or procedure columns.
#'
#' @param data A DuckDB-backed lazy table returned by [nrd_ingest()].
#' @param name Name of the output logical flag column.
#' @param pattern Optional regular expression applied directly to data values.
#' @param codes Optional character vector of exact codes to match.
#' @param type Either `"dx"` or `"pr"`.
#' @param scope One of `"principal"`, `"secondary"`, or `"all"`.
#'
#' @returns A DuckDB-backed `tbl_lazy` with one additional logical column.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/nrd.parquet")
#'   flagged <- nrd_flag_condition(data, "is_ami", pattern = "^I21", type = "dx", scope = "all")
#' }
nrd_flag_condition <- function(
  data,
  name,
  pattern = NULL,
  codes = NULL,
  type,
  scope
) {
  .nrd_assert_duckdb_lazy(data)

  if (!is.character(name) || length(name) != 1 || is.na(name) || !nzchar(name)) {
    rlang::abort("`name` must be a single, non-empty string.")
  }

  has_pattern <- !is.null(pattern)
  has_codes <- !is.null(codes)
  if (identical(has_pattern, has_codes)) {
    rlang::abort("Exactly one of `pattern` or `codes` must be supplied.")
  }

  if (has_pattern && (!is.character(pattern) || length(pattern) != 1 || is.na(pattern) || !nzchar(pattern))) {
    rlang::abort("`pattern` must be a single, non-empty string when supplied.")
  }

  if (has_codes && (!is.character(codes) || length(codes) == 0 || any(is.na(codes)))) {
    rlang::abort("`codes` must be a non-empty character vector with no missing values.")
  }

  type <- rlang::arg_match(type, c("dx", "pr"))
  scope <- rlang::arg_match(scope, c("principal", "secondary", "all"))

  cols <- .nrd_condition_columns(data, type = type, scope = scope)
  predicate <- .nrd_condition_predicate(cols, pattern = pattern, codes = codes)

  dplyr::mutate(
    data,
    !!rlang::sym(name) := dplyr::coalesce(!!predicate, FALSE)
  )
}
