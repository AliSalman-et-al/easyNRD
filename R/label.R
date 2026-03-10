#' Translate HCUP Numeric Codes to Character Labels
#'
#' `nrd_label()` translates standard HCUP integer-coded variables into
#' human-readable character labels using dictionary mappings stored in
#' `easyNRD` internals. For lazy database backends, translations are expressed
#' with `dplyr::case_match()` so they push down to SQL `CASE WHEN` operations
#' and run out-of-core before materialization.
#'
#' @param .data A data frame, lazy table, or Arrow query.
#' @param ... <[`tidy-select`][dplyr::dplyr_tidy_select]> Columns to label.
#'   If omitted, all columns present in `.data` that have dictionary mappings
#'   are labeled automatically.
#' @param .keep_original Logical scalar. If `FALSE` (default), overwrite the
#'   selected coded columns. If `TRUE`, keep the original coded columns and
#'   append labeled columns with the `_label` suffix.
#'
#' @return A modified object of the same backend class as `.data`.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_select(FEMALE, PAY1, AGE) |>
#'     nrd_label(FEMALE, PAY1) |>
#'     nrd_as_survey()
#' }
nrd_label <- function(.data, ..., .keep_original = FALSE) {
  dict_keys <- names(nrd_dict)
  selected_quos <- rlang::enquos(...)

  selected_cols <- if (length(selected_quos) == 0) {
    intersect(colnames(.data), dict_keys)
  } else {
    requested_cols <- names(tidyselect::eval_select(
      expr = rlang::expr(c(!!!selected_quos)),
      data = .data
    ))

    missing_dict <- setdiff(requested_cols, dict_keys)
    if (length(missing_dict) > 0) {
      rlang::warn(paste0(
        "Requested columns have no label dictionary and were skipped: ",
        paste(missing_dict, collapse = ", ")
      ))
    }

    intersect(requested_cols, dict_keys)
  }

  if (length(selected_cols) == 0) {
    return(.data)
  }

  out <- .data

  for (col in selected_cols) {
    dict <- nrd_dict[[col]]
    col_sym <- rlang::sym(col)
    match_exprs <- unname(lapply(seq_along(dict), function(i) {
      key <- names(dict)[i]
      val <- dict[[i]]
      rlang::expr(!!key ~ !!val)
    }))

    output_col <- if (.keep_original) {
      rlang::sym(paste0(col, "_label"))
    } else {
      col_sym
    }

    out <- dplyr::mutate(
      out,
      !!output_col := dplyr::case_match(
        as.character(!!col_sym),
        !!!match_exprs,
        .default = as.character(!!col_sym)
      )
    )
  }

  out
}
