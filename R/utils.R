.nrd_assert_cols <- function(.data, required_cols) {
  missing_cols <- setdiff(required_cols, colnames(.data))
  if (length(missing_cols) > 0) {
    rlang::abort(
      paste0("Missing required columns: ", paste(missing_cols, collapse = ", "))
    )
  }
}

.nrd_is_lazy_table <- function(.data) {
  inherits(.data, "tbl_lazy") || inherits(.data, "arrow_dplyr_query")
}

.nrd_is_duckdb_lazy <- function(.data) {
  if (!.nrd_is_lazy_table(.data)) {
    return(FALSE)
  }

  con <- tryCatch(dbplyr::remote_con(.data), error = function(e) NULL)
  inherits(con, "duckdb_connection")
}

.nrd_assert_lazy_duckdb <- function(.data, arg = "data") {
  if (!.nrd_is_lazy_table(.data)) {
    rlang::abort(
      paste0(
        "`", arg, "` must be a lazy DuckDB table. ",
        "Use `nrd_ingest()` to initialize an out-of-core pipeline."
      )
    )
  }

  if (!.nrd_is_duckdb_lazy(.data)) {
    rlang::abort(
      paste0(
        "`", arg, "` must use a DuckDB backend. ",
        "Use `nrd_ingest()` to initialize an out-of-core pipeline."
      )
    )
  }

  invisible(.data)
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

#' Resolve the easyNRD Cache Directory
#'
#' `nrd_cache_dir()` resolves the package cache directory using a deterministic
#' fallback hierarchy. This path is used for persistent out-of-core artifacts,
#' including DuckDB temporary spill files in large NRD workflows.
#'
#' Resolution order:
#' 1. `EASYNRD_CACHE_DIR` environment variable
#' 2. `getOption("easynrd.cache_dir")`
#' 3. `tools::R_user_dir("easyNRD", which = "cache")`
#'
#' The resolved directory is created if it does not already exist.
#'
#' @returns A single character path to the resolved cache directory.
#' @export
nrd_cache_dir <- function() {
  env_path <- Sys.getenv("EASYNRD_CACHE_DIR", unset = "")
  if (nzchar(env_path)) {
    cache_path <- env_path
  } else {
    opt_path <- getOption("easynrd.cache_dir")
    if (is.character(opt_path) && length(opt_path) == 1 && !is.na(opt_path) && nzchar(opt_path)) {
      cache_path <- opt_path
    } else {
      cache_path <- tools::R_user_dir("easyNRD", which = "cache")
    }
  }

  dir.create(cache_path, recursive = TRUE, showWarnings = FALSE)
  cache_path
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

  if (.nrd_is_lazy_table(.data)) {
    dx_combined_expr <- if (length(dx_cols) > 0) {
      rlang::parse_expr(
        paste0("CONCAT_WS(', ', ", paste(dx_cols, collapse = ", "), ")")
      )
    } else {
      rlang::expr(NA_character_)
    }

    pr_combined_expr <- if (length(pr_cols) > 0) {
      rlang::parse_expr(
        paste0("CONCAT_WS(', ', ", paste(pr_cols, collapse = ", "), ")")
      )
    } else {
      rlang::expr(NA_character_)
    }

    return(dplyr::mutate(
      .data,
      DX10_Combined = !!dx_combined_expr,
      PR10_Combined = !!pr_combined_expr
    ))
  }

  local_concat_ws <- function(.tbl, cols) {
    if (length(cols) == 0) {
      return(rep(NA_character_, nrow(.tbl)))
    }

    mat <- as.data.frame(dplyr::select(.tbl, dplyr::all_of(cols)))
    apply(mat, 1, function(x) {
      vals <- x[!is.na(x) & nzchar(x)]
      if (length(vals) == 0) NA_character_ else paste(vals, collapse = ", ")
    })
  }

  dplyr::mutate(
    .data,
    DX10_Combined = local_concat_ws(.data, dx_cols),
    PR10_Combined = local_concat_ws(.data, pr_cols)
  )
}

.nrd_add_year_end_censoring <- function(
  .data,
  window = 30L,
  censor_method = c("drop_month", "mid_month")
) {
  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)
  censor_method <- rlang::arg_match(censor_method)

  month_lookup <- tibble::tibble(
    Episode_DMONTH = 1:12,
    .nrd_month_start = c(0L, 31L, 59L, 90L, 120L, 151L, 181L, 212L, 243L, 273L, 304L, 334L),
    .nrd_month_end = c(31L, 59L, 90L, 120L, 151L, 181L, 212L, 243L, 273L, 304L, 334L, 365L)
  )

  out <- .data |>
    dplyr::left_join(month_lookup, by = "Episode_DMONTH", copy = TRUE) |>
    dplyr::mutate(
      .nrd_is_leap = (YEAR %% 400L == 0L) | (YEAR %% 4L == 0L & YEAR %% 100L != 0L),
      .nrd_year_days = dplyr::if_else(.nrd_is_leap, 366L, 365L),
      .nrd_month_start = dplyr::if_else(
        .nrd_is_leap & Episode_DMONTH > 2L,
        .nrd_month_start + 1L,
        .nrd_month_start
      ),
      .nrd_month_end = dplyr::if_else(
        .nrd_is_leap & Episode_DMONTH > 2L,
        .nrd_month_end + 1L,
        .nrd_month_end
      ),
      .nrd_approx_discharge_doy = .nrd_month_start + 15L,
      Days_to_End_of_Year = .nrd_year_days - .nrd_approx_discharge_doy,
      .nrd_followup_complete = .nrd_month_end <= (.nrd_year_days - window)
    )

  if (identical(censor_method, "mid_month")) {
    return(out |>
      dplyr::select(
        -.nrd_month_start,
        -.nrd_month_end,
        -.nrd_is_leap,
        -.nrd_year_days,
        -.nrd_approx_discharge_doy
      ))
  }

  out |>
    dplyr::select(
      -.nrd_month_start,
      -.nrd_month_end,
      -.nrd_is_leap,
      -.nrd_year_days,
      -.nrd_approx_discharge_doy
    )
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

.nrd_list_session_temp_dirs <- function(temp_directory) {
  if (!dir.exists(temp_directory)) {
    return(character(0))
  }

  dirs <- list.dirs(temp_directory, recursive = FALSE, full.names = TRUE)
  dirs[grepl("^easyNRD_[0-9]+$", basename(dirs))]
}

.nrd_extract_session_pid <- function(dir_path) {
  dir_name <- basename(dir_path)
  pid_chr <- sub("^easyNRD_", "", dir_name)
  suppressWarnings(as.integer(pid_chr))
}

.nrd_pid_is_alive <- function(pid) {
  if (!is.numeric(pid) || length(pid) != 1 || is.na(pid) || pid <= 0) {
    return(FALSE)
  }

  isTRUE(tryCatch(
    tools::pskill(as.integer(pid), 0),
    error = function(e) FALSE,
    warning = function(w) FALSE
  ))
}

.nrd_remove_dir_safe <- function(dir_path) {
  tryCatch(
    {
      unlink(dir_path, recursive = TRUE, force = TRUE)
      !dir.exists(dir_path)
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
}

.nrd_cleanup_session_temp_dirs <- function(temp_directory, force = FALSE) {
  dirs <- .nrd_list_session_temp_dirs(temp_directory)
  if (length(dirs) == 0) {
    return(list(removed = 0L, discovered = 0L, candidates = 0L))
  }

  if (isTRUE(force)) {
    dirs_to_remove <- dirs
  } else {
    pids <- vapply(dirs, .nrd_extract_session_pid, FUN.VALUE = integer(1))
    is_alive <- vapply(pids, .nrd_pid_is_alive, FUN.VALUE = logical(1))
    dirs_to_remove <- dirs[!is_alive]
  }

  if (length(dirs_to_remove) == 0) {
    return(list(
      removed = 0L,
      discovered = as.integer(length(dirs)),
      candidates = 0L
    ))
  }

  removed <- sum(vapply(dirs_to_remove, .nrd_remove_dir_safe, FUN.VALUE = logical(1)))

  list(
    removed = as.integer(removed),
    discovered = as.integer(length(dirs)),
    candidates = as.integer(length(dirs_to_remove))
  )
}
