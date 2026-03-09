.nrd_assert_cols <- function(.data, required_cols) {
  missing_cols <- setdiff(required_cols, colnames(.data))
  if (length(missing_cols) > 0) {
    rlang::abort(
      paste0("Missing required columns: ", paste(missing_cols, collapse = ", "))
    )
  }
}

.nrd_standardize_names <- function(.data) {
  rename_map <- c(
    "NRD_VisitLink" = "NRD_VISITLINK",
    "NRD_DaysToEvent" = "NRD_DAYSTOEVENT"
  )

  present <- names(rename_map)[names(rename_map) %in% colnames(.data)]
  if (length(present) == 0) {
    return(.data)
  }

  dplyr::rename(.data, !!!stats::setNames(present, rename_map[present]))
}

nrd_window_order <- function(.data, ...) {
  if (inherits(.data, "tbl_lazy")) {
    dbplyr::window_order(.data, ...)
  } else {
    dplyr::arrange(.data, ..., .by_group = TRUE)
  }
}

.nrd_extract_codes <- function(.data) {
  dx_cols <- names(tidyselect::eval_select(
    rlang::expr(tidyselect::starts_with("I10_DX")),
    data = .data
  ))
  pr_cols <- names(tidyselect::eval_select(
    rlang::expr(tidyselect::starts_with("I10_PR")),
    data = .data
  ))

  dx_combined_expr <- if (length(dx_cols) > 0) {
    rlang::expr(stringr::str_c(!!!rlang::syms(dx_cols), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  pr_combined_expr <- if (length(pr_cols) > 0) {
    rlang::expr(stringr::str_c(!!!rlang::syms(pr_cols), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  dplyr::mutate(
    .data,
    DX10_Combined = !!dx_combined_expr,
    PR10_Combined = !!pr_combined_expr
  )
}

.nrd_add_year_end_censoring <- function(.data) {
  month_lookup <- tibble::tibble(
    Episode_DMONTH = 1:12,
    .nrd_month_start = c(0L, 31L, 59L, 90L, 120L, 151L, 181L, 212L, 243L, 273L, 304L, 334L)
  )

  .data |>
    dplyr::left_join(month_lookup, by = "Episode_DMONTH", copy = TRUE) |>
    dplyr::mutate(
      .nrd_is_leap = (YEAR %% 400L == 0L) | (YEAR %% 4L == 0L & YEAR %% 100L != 0L),
      .nrd_year_days = dplyr::if_else(.nrd_is_leap, 366L, 365L),
      .nrd_month_start = dplyr::if_else(
        .nrd_is_leap & Episode_DMONTH > 2L,
        .nrd_month_start + 1L,
        .nrd_month_start
      ),
      .nrd_approx_discharge_doy = .nrd_month_start + 15L,
      Days_to_End_of_Year = .nrd_year_days - .nrd_approx_discharge_doy
    ) |>
    dplyr::select(-.nrd_month_start, -.nrd_is_leap, -.nrd_year_days, -.nrd_approx_discharge_doy)
}

.nrd_resolve_readmit_vars <- function(.data, readmit_vars) {
  vars_quo <- rlang::enquo(readmit_vars)
  if (rlang::quo_is_null(vars_quo)) {
    return(character(0))
  }
  names(tidyselect::eval_select(vars_quo, data = .data))
}

.nrd_double_map <- function(.data, vars) {
  vars <- unique(intersect(vars, colnames(.data)))
  if (length(vars) == 0) {
    return(stats::setNames(logical(0), character(0)))
  }

  out <- stats::setNames(rep(NA, length(vars)), vars)

  if (inherits(.data, "tbl_lazy") || inherits(.data, "arrow_dplyr_query")) {
    schema <- tryCatch(
      {
        .data |>
          dplyr::select(dplyr::all_of(vars)) |>
          dplyr::slice_head(n = 0) |>
          dplyr::collect()
      },
      error = function(e) NULL
    )

    if (is.null(schema)) {
      return(out)
    }

    for (v in intersect(vars, names(schema))) {
      out[[v]] <- is.double(schema[[v]])
    }

    return(out)
  }

  for (v in vars) {
    out[[v]] <- is.double(.data[[v]])
  }

  out
}

.nrd_should_sum_var <- function(.data, var, double_map = NULL) {
  if (grepl("(CHG|COST|PAY|LOS|DAY)", var, ignore.case = TRUE)) {
    return(TRUE)
  }

  default_sum_vars <- c("Episode_TOTCHG", "TOTCHG")
  if (var %in% default_sum_vars) {
    return(TRUE)
  }

  if (!is.null(double_map) && var %in% names(double_map) && !is.na(double_map[[var]])) {
    return(isTRUE(double_map[[var]]))
  }

  if (!inherits(.data, "tbl_lazy") && var %in% colnames(.data)) {
    col <- .data[[var]]
    if (is.double(col)) {
      return(TRUE)
    }
  }

  FALSE
}
