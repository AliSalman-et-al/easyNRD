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

nrd_augment_labels <- function(.data) {
  out <- .data
  present <- .nrd_lookup_registry |>
    dplyr::filter(.data$column %in% colnames(out))

  if (nrow(present) == 0) {
    return(out)
  }

  for (i in seq_len(nrow(present))) {
    src_col <- present$column[[i]]
    lookup_tbl <- .nrd_fetch_lookup(present$lookup[[i]])
    out_col <- paste0(src_col, "_label")
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

nrd_extract_codes <- function(.data) {
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

.nrd_read_required_cols <- c(
  "YEAR", "NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT", "LOS",
  "DISPUNIFORM", "SAMEDAYEVENT", "DMONTH", "DIED", "I10_DX1"
)

.nrd_assert_cols <- function(.data, required_cols) {
  missing_cols <- setdiff(required_cols, colnames(.data))
  if (length(missing_cols) > 0) {
    rlang::abort(
      paste0("Missing required columns: ", paste(missing_cols, collapse = ", "))
    )
  }
}

#' Read and Assemble NRD Episodes Lazily
#'
#' `nrd_read()` is the unified ingestion entrypoint for HCUP-NRD. It reads
#' parquet input lazily, augments administrative labels through relational
#' lookups, constructs diagnosis/procedure row strings, and deterministically
#' consolidates transfer-linked rows into episode-level records.
#'
#' @details
#' The Nationwide Readmissions Database is hospitalization-level. Transfer chains
#' can span multiple rows but represent one clinical episode. `nrd_read()` uses a
#' strict temporal frame (`NRD_DAYSTOEVENT`, `KEY_NRD`) within
#' `YEAR` + `NRD_VISITLINK`, builds transfer continuity using `DISPUNIFORM` and
#' `SAMEDAYEVENT`, and collapses each chain to one episode row.
#'
#' Episode-level diagnosis and procedure arrays are aggregated across the
#' transfer chain as `Episode_DX10` and `Episode_PR10`. The principal diagnosis
#' of the unified episode (`Episode_DX10_Principal`) is taken from the first row
#' in sequence (`I10_DX1`).
#'
#' This function never filters the denominator and is designed for survey-aware,
#' lazy pipelines.
#'
#' @param datasets A parquet path, Arrow dataset, or lazy table.
#' @param transfer_disp_codes Integer vector of transfer-out `DISPUNIFORM` codes.
#'   Defaults to `2L`.
#' @param transfer_sameday_codes Integer vector of `SAMEDAYEVENT` continuity
#'   codes. Defaults to `c(1L, 4L)`.
#'
#' @return A lazy episode-level table.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   episodes <- nrd_read("/path/to/nrd/parquet")
#' }
#' }
nrd_read <- function(
  datasets,
  transfer_disp_codes = 2L,
  transfer_sameday_codes = c(1L, 4L)
) {
  tbl <- if (inherits(datasets, "tbl_lazy") || inherits(datasets, "ArrowTabular")) {
    datasets
  } else {
    arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
      arrow::to_duckdb()
  }

  .nrd_assert_cols(tbl, .nrd_read_required_cols)

  base_augmented_tbl <- tbl |>
    nrd_augment_labels() |>
    nrd_extract_codes()

  row_level <- base_augmented_tbl |>
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

  row_keyed <- row_level |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_ID) |>
    nrd_window_order(NRD_DAYSTOEVENT, KEY_NRD) |>
    dplyr::mutate(
      .nrd_row_number = dplyr::row_number(),
      .nrd_episode_rows = dplyr::n(),
      .nrd_dx1_first = dplyr::if_else(.nrd_row_number == 1L, I10_DX1, NA_character_),
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

  row_keyed |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_ID) |>
    dplyr::summarise(
      Episode_Index_KEY_NRD = min(Episode_Index_KEY_NRD, na.rm = TRUE),
      Episode_KEY_NRD = max(Episode_KEY_NRD, na.rm = TRUE),
      Episode_DMONTH = max(Episode_DMONTH, na.rm = TRUE),
      Episode_Admission_Day = min(.nrd_admit_day, na.rm = TRUE),
      Episode_LOS = sum(.nrd_los, na.rm = TRUE),
      Episode_TOTCHG = sum(as.numeric(TOTCHG), na.rm = TRUE),
      Episode_DX10 = stringr::str_flatten(DX10_Combined, collapse = ", "),
      Episode_PR10 = stringr::str_flatten(PR10_Combined, collapse = ", "),
      Episode_DX10_Principal = max(.nrd_dx1_first, na.rm = TRUE),
      DIED = max(DIED, na.rm = TRUE),
      Episode_N_Stays = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(Episode_Discharge_Day = Episode_Admission_Day + Episode_LOS) |>
    dplyr::left_join(
      base_augmented_tbl |>
        dplyr::select(
          -dplyr::any_of(c(
            "DIED", "LOS", "TOTCHG", "DMONTH", "NRD_DAYSTOEVENT",
            "DISPUNIFORM", "SAMEDAYEVENT", "NRD_VISITLINK"
          )),
          -dplyr::starts_with("I10_DX"),
          -dplyr::starts_with("I10_PR"),
          -dplyr::starts_with("DX10_"),
          -dplyr::starts_with("PR10_")
        ),
      by = c("YEAR", "Episode_Index_KEY_NRD" = "KEY_NRD")
    )
}
