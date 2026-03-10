#' Convert Labeled NRD Variables to Ordered Factors
#'
#' `nrd_factorize()` converts labeled HCUP variables to R factors using the
#' canonical level order stored in the internal NRD dictionary. This ensures
#' stable reference categories for downstream modeling workflows such as
#' `survival` and `gtsummary`.
#'
#' @param data A local in-memory `data.frame`/tibble or `tbl_svy` object.
#'
#' @return The input object with dictionary-backed columns converted to factors.
#' @export
#'
#' @examples
#' dat <- tibble::tibble(
#'   FEMALE = c("Male", "Female"),
#'   PAY1 = c("Medicare", "Private"),
#'   AGE = c(65, 72)
#' )
#'
#' nrd_factorize(dat)
nrd_factorize <- function(data) {
  if (inherits(data, "tbl_svy")) {
    if (.nrd_is_lazy_table(data$variables)) {
      rlang::abort(
        "`data` is a lazy survey object. Call `dplyr::collect()` before `nrd_factorize()`."
      )
    }

    dict_cols <- intersect(names(data$variables), names(nrd_dict))
  } else if (is.data.frame(data)) {
    if (.nrd_is_lazy_table(data)) {
      rlang::abort(
        "`data` is lazy. Call `dplyr::collect()` before `nrd_factorize()`."
      )
    }

    dict_cols <- intersect(names(data), names(nrd_dict))
  } else {
    rlang::abort("`data` must be an in-memory data frame/tibble or `tbl_svy` object.")
  }

  if (length(dict_cols) == 0) {
    return(data)
  }

  out <- data

  for (col in dict_cols) {
    col_sym <- rlang::sym(col)
    level_order <- unname(nrd_dict[[col]])

    out <- dplyr::mutate(
      out,
      !!col_sym := factor(as.character(!!col_sym), levels = level_order)
    )
  }

  out
}
