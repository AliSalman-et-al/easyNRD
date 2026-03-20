#' Convert Labeled NRD Variables to Ordered Factors
#'
#' `nrd_factorize()` converts labeled HCUP variables to R factors using the
#' canonical level order stored in the internal NRD dictionary. This ensures
#' stable reference categories for downstream modeling workflows such as
#' `survival` and `gtsummary`.
#'
#' @param data A local in-memory `data.frame`/tibble or `tbl_svy` object.
#'
#' @returns The input object with dictionary-backed columns converted to factors.
#' @export
#'
#' @examples
#' dat <- dplyr::tibble(
#'   FEMALE = c("Male", "Female"),
#'   PAY1 = c("Medicare", "Private"),
#'   AGE = c(65, 72)
#' )
#'
#' nrd_factorize(dat)
nrd_factorize <- function(data) {
  out <- data

  if (inherits(data, "tbl_svy")) {
    if (.nrd_is_lazy_table(data$variables)) {
      rlang::abort(
        "`data` is a lazy survey object. Call `dplyr::collect()` before `nrd_factorize()`."
      )
    }

    available_cols <- names(data$variables)
  } else if (is.data.frame(data)) {
    if (.nrd_is_lazy_table(data)) {
      rlang::abort(
        "`data` is lazy. Call `dplyr::collect()` before `nrd_factorize()`."
      )
    }

    available_cols <- names(data)
  } else {
    rlang::abort("`data` must be an in-memory data frame/tibble or `tbl_svy` object.")
  }

  mutations <- rlang::list2()

  for (base_col in names(nrd_dict)) {
    label_col <- paste0(base_col, "_label")

    target_col <- if (label_col %in% available_cols) {
      label_col
    } else if (base_col %in% available_cols) {
      base_col
    } else {
      NULL
    }

    if (is.null(target_col)) {
      next
    }

    col_sym <- rlang::sym(target_col)
    level_order <- unname(nrd_dict[[base_col]])

    mutations[[target_col]] <- rlang::expr(
      factor(as.character(!!col_sym), levels = !!level_order)
    )
  }

  if (length(mutations) == 0) {
    return(data)
  }

  dplyr::mutate(out, !!!mutations)
}
