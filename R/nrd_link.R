#' Deprecated: Use `nrd_link_readmissions()`
#'
#' `nrd_link()` is retained for backward compatibility and forwards directly to
#' `nrd_link_readmissions()`.
#'
#' @param .data Episode-level lazy table from `nrd_build_episodes()`.
#' @param index_condition Unquoted logical expression that defines index events.
#' @param readmit_condition Unquoted logical expression that defines qualifying
#'   readmission events.
#' @param window Integer readmission window in days. Defaults to `30L`.
#' @param readmit_vars Optional tidyselect specification for columns to pull from
#'   the linked readmission and append as wide `readmit_*` columns.
#' @return A denominator-preserving lazy table with readmission outcomes.
#' @export
nrd_link <- function(
  .data,
  index_condition,
  readmit_condition,
  window = 30L,
  readmit_vars = NULL
) {
  .Deprecated(new = "nrd_link_readmissions")
  nrd_link_readmissions(
    data = .data,
    index_condition = {{ index_condition }},
    readmit_condition = {{ readmit_condition }},
    window = window,
    readmit_vars = {{ readmit_vars }}
  )
}
