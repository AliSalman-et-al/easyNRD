.nrd_lookup_registry <- tibble::tribble(
  ~column, ~lookup,
  "FEMALE", "nrd_lookup_sex",
  "ZIPINC_QRTL", "nrd_lookup_income_quartile",
  "PAY1", "nrd_lookup_payer",
  "PL_NCHS", "nrd_lookup_urbanicity",
  "H_CONTRL", "nrd_lookup_h_contrl",
  "HOSP_BEDSIZE", "nrd_lookup_hosp_bedsize",
  "HOSP_UR_TEACH", "nrd_lookup_hosp_ur_teach",
  "HOSP_URCAT4", "nrd_lookup_hosp_urcat4",
  "DISPUNIFORM", "nrd_lookup_dispuniform",
  "SAMEDAYEVENT", "nrd_lookup_samedayevent"
)

.nrd_duckdb_driver <- function() {
  duckdb::duckdb()
}

nrd_window_order <- function(.data, ...) {
  if (inherits(.data, "tbl_lazy")) {
    dbplyr::window_order(.data, ...)
  } else {
    dplyr::arrange(.data, ..., .by_group = TRUE)
  }
}

.nrd_fetch_lookup <- function(name) {
  lookup_env <- if (exists(".__NAMESPACE__.", inherits = FALSE)) {
    asNamespace("easyNRD")
  } else {
    parent.frame()
  }

  if (!exists(name, envir = lookup_env, inherits = TRUE)) {
    rlang::abort(
      paste0(
        "Internal lookup table `", name, "` was not found. ",
        "Run `source('data-raw/lookup_tables.R')` from the package root to rebuild internal lookup data."
      )
    )
  }

  get(name, envir = lookup_env, inherits = TRUE)
}

#' Join NRD administrative labels from internal lookup tables
#'
#' Internal helper that enriches code columns by joining relational lookup
#' tables. This function is intentionally unexported.
#'
#' @param .data A lazy NRD table.
#' @param keep_original Logical; if `TRUE`, append `<column>_label`.
#'
#' @return Input table with label columns joined.
#' @noRd
nrd_augment_labels <- function(.data, keep_original = TRUE) {
  out <- .data

  present <- .nrd_lookup_registry |>
    dplyr::filter(.data$column %in% colnames(out))

  if (nrow(present) == 0) {
    return(out)
  }

  for (i in seq_len(nrow(present))) {
    src_col <- present$column[[i]]
    lookup_tbl <- .nrd_fetch_lookup(present$lookup[[i]])
    out_col <- if (keep_original) paste0(src_col, "_label") else src_col

    out <- out |>
      dplyr::left_join(
        lookup_tbl,
        by = stats::setNames("code", src_col),
        copy = TRUE
      ) |>
      dplyr::rename(.nrd_label = label) |>
      dplyr::mutate(!!out_col := .data$.nrd_label) |>
      dplyr::select(-.data$.nrd_label)
  }

  out
}

#' Dynamically combine diagnosis and procedure columns
#'
#' Internal helper that detects variable-width ICD fields and produces combined
#' strings without hardcoded terminal positions. This function is intentionally
#' unexported.
#'
#' @param .data A lazy NRD table.
#' @param dx_prefix Diagnosis prefix.
#' @param pr_prefix Procedure prefix.
#' @param prday_prefix Procedure-day prefix.
#'
#' @return Input table with combined DX/PR columns appended.
#' @noRd
nrd_extract_codes <- function(
  .data,
  dx_prefix = "I10_DX",
  pr_prefix = "I10_PR",
  prday_prefix = "PRDAY"
) {
  dx_cols <- names(tidyselect::eval_select(
    rlang::expr(tidyselect::starts_with(!!dx_prefix)),
    data = .data
  ))
  pr_cols <- names(tidyselect::eval_select(
    rlang::expr(tidyselect::starts_with(!!pr_prefix)),
    data = .data
  ))
  prday_cols <- names(tidyselect::eval_select(
    rlang::expr(tidyselect::starts_with(!!prday_prefix)),
    data = .data
  ))

  dx_combined_expr <- if (length(dx_cols) > 0) {
    rlang::expr(stringr::str_c(!!!rlang::syms(dx_cols), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  dx_secondary_expr <- if (length(dx_cols) > 1) {
    rlang::expr(stringr::str_c(!!!rlang::syms(dx_cols[-1]), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  pr_combined_expr <- if (length(pr_cols) > 0) {
    rlang::expr(stringr::str_c(!!!rlang::syms(pr_cols), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  pr_secondary_expr <- if (length(pr_cols) > 1) {
    rlang::expr(stringr::str_c(!!!rlang::syms(pr_cols[-1]), sep = ", "))
  } else {
    rlang::expr(NA_character_)
  }

  out <- dplyr::mutate(
    .data,
    DX10_Combined = !!dx_combined_expr,
    DX10_Secondary = !!dx_secondary_expr,
    PR10_Combined = !!pr_combined_expr,
    PR10_Secondary = !!pr_secondary_expr
  )

  if (length(prday_cols) > 0) {
    out <- out |>
      dplyr::mutate(dplyr::across(dplyr::any_of(prday_cols), as.numeric))
  }

  out
}

.nrd_episode_required_cols <- c(
  "YEAR", "NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT", "LOS",
  "DISPUNIFORM", "SAMEDAYEVENT", "DMONTH", "DIED"
)

.nrd_assert_cols <- function(.data, required_cols) {
  missing_cols <- setdiff(required_cols, colnames(.data))
  if (length(missing_cols) > 0) {
    rlang::abort(
      paste0("Missing required columns: ", paste(missing_cols, collapse = ", "))
    )
  }
}

.nrd_build_episode_table <- function(
  .data,
  transfer_disp_codes,
  transfer_sameday_codes
) {
  row_level <- .data |>
    dplyr::group_by(YEAR, NRD_VISITLINK) |>
    nrd_window_order(NRD_DAYSTOEVENT, KEY_NRD) |>
    dplyr::mutate(
      .nrd_admit_day = as.numeric(NRD_DAYSTOEVENT),
      .nrd_los = as.numeric(LOS),
      .nrd_discharge_day_row = .nrd_admit_day + .nrd_los,
      .nrd_transfer_trigger = DISPUNIFORM %in% transfer_disp_codes &
        SAMEDAYEVENT %in% transfer_sameday_codes,
      .nrd_prev_trigger = dplyr::lag(.nrd_transfer_trigger, default = FALSE),
      .nrd_prev_discharge = dplyr::lag(.nrd_discharge_day_row),
      .nrd_contiguous = dplyr::coalesce(.nrd_prev_trigger, FALSE) &
        !is.na(.nrd_admit_day) &
        !is.na(.nrd_prev_discharge) &
        .nrd_admit_day == .nrd_prev_discharge,
      Episode_ID = cumsum(dplyr::if_else(.nrd_contiguous, 0L, 1L))
    ) |>
    dplyr::ungroup()

  row_with_keys <- row_level |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_ID) |>
    nrd_window_order(NRD_DAYSTOEVENT, KEY_NRD) |>
    dplyr::mutate(
      .nrd_row_number = dplyr::row_number(),
      .nrd_episode_rows = dplyr::n(),
      Episode_Index_KEY_NRD = min(
        dplyr::if_else(.nrd_row_number == 1L, KEY_NRD, NA_integer_),
        na.rm = TRUE
      ),
      Episode_KEY_NRD = max(
        dplyr::if_else(.nrd_row_number == .nrd_episode_rows, KEY_NRD, NA_integer_),
        na.rm = TRUE
      ),
      Episode_DMONTH = max(
        dplyr::if_else(.nrd_row_number == .nrd_episode_rows, DMONTH, NA_integer_),
        na.rm = TRUE
      )
    ) |>
    dplyr::ungroup()

  summary_exprs <- list(
    Episode_Index_KEY_NRD = rlang::expr(min(Episode_Index_KEY_NRD, na.rm = TRUE)),
    Episode_KEY_NRD = rlang::expr(max(Episode_KEY_NRD, na.rm = TRUE)),
    Episode_DMONTH = rlang::expr(max(Episode_DMONTH, na.rm = TRUE)),
    Episode_Admission_Day = rlang::expr(min(.nrd_admit_day, na.rm = TRUE)),
    Episode_LOS = rlang::expr(sum(.nrd_los, na.rm = TRUE)),
    DIED = rlang::expr(max(DIED, na.rm = TRUE)),
    Episode_N_Stays = rlang::expr(dplyr::n())
  )

  if ("TOTCHG" %in% colnames(row_with_keys)) {
    summary_exprs$Episode_TOTCHG <- rlang::expr(sum(TOTCHG, na.rm = TRUE))
  }

  row_with_keys |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_ID) |>
    dplyr::summarise(!!!summary_exprs, .groups = "drop") |>
    dplyr::mutate(Episode_Discharge_Day = Episode_Admission_Day + Episode_LOS)
}

#' Assemble NRD Clinical Episodes from Raw Hospitalization Rows
#'
#' Ingests HCUP-NRD parquet data lazily, augments administrative labels, extracts
#' diagnosis/procedure code strings, and deterministically collapses transfer-
#' linked rows into clinical episodes.
#'
#' @details
#' HCUP-NRD is a hospitalization-level administrative dataset. Transfer networks
#' can create multiple rows for one clinical episode; therefore, episode
#' consolidation is mandatory before any readmission or survival modeling.
#'
#' The consolidation procedure is SQL-safe for lazy backends and uses only
#' deterministic ordering plus stable aggregates:
#'
#' - grouping frame: `YEAR` + `NRD_VISITLINK`
#' - temporal frame: `dbplyr::window_order(NRD_DAYSTOEVENT, KEY_NRD)`
#' - chain continuity requires transfer disposition and same-day network codes,
#'   with exact day-to-day adjacency between prior discharge and current
#'   admission
#' - `Episode_Index_KEY_NRD` and `Episode_KEY_NRD` are broadcast in a windowed
#'   mutate before summarization; `dplyr::first()`/`dplyr::last()` are not used
#'   in `summarise()`.
#'
#' Final episode variables include:
#'
#' - `Episode_Admission_Day = min(NRD_DAYSTOEVENT)`
#' - `Episode_LOS = sum(LOS, na.rm = TRUE)`
#' - `Episode_Discharge_Day = Episode_Admission_Day + Episode_LOS`
#' - `DIED = max(DIED, na.rm = TRUE)`
#'
#' This function preserves lazy execution in Arrow/DuckDB workflows and is
#' designed to support denominator-preserving survey-based analyses.
#'
#' @param datasets A parquet path, Arrow dataset, or lazy table.
#' @param unify_schemas Logical passed to `arrow::open_dataset()` when
#'   `datasets` is a parquet path.
#' @param transfer_disp_codes Integer vector of `DISPUNIFORM` values indicating
#'   transfer-out events. Defaults to `2L`.
#' @param transfer_sameday_codes Integer vector of `SAMEDAYEVENT` values
#'   indicating transfer-network continuity. Defaults to `c(1L, 4L)`.
#'
#' @return A lazy episode-level table.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   episodes <- nrd_assemble_episodes(
#'     datasets = "/path/to/nrd/parquet",
#'     transfer_disp_codes = 2L,
#'     transfer_sameday_codes = c(1L, 4L)
#'   )
#' }
#' }
nrd_assemble_episodes <- function(
  datasets,
  unify_schemas = TRUE,
  transfer_disp_codes = 2L,
  transfer_sameday_codes = c(1L, 4L)
) {
  tbl <- if (inherits(datasets, "tbl_lazy") || inherits(datasets, "ArrowTabular")) {
    datasets
  } else {
    arrow::open_dataset(
      datasets,
      unify_schemas = unify_schemas,
      format = "parquet"
    ) |>
      arrow::to_duckdb()
  }

  .nrd_assert_cols(tbl, .nrd_episode_required_cols)

  tbl |>
    nrd_augment_labels(keep_original = TRUE) |>
    nrd_extract_codes() |>
    .nrd_build_episode_table(
      transfer_disp_codes = transfer_disp_codes,
      transfer_sameday_codes = transfer_sameday_codes
    )
}
