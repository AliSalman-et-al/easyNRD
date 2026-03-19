#' Consolidate NRD Discharges into Clinical Episodes
#'
#' `nrd_build_episodes()` deterministically consolidates transfer-linked and
#' same-day hospital records into one clinical episode per trajectory segment.
#' The result remains lazy so users can continue phenotyping with
#' `dplyr::mutate()` before modeling.
#'
#' @param .data A lazy NRD table from [nrd_ingest()].
#' @param transfer_disp_codes Integer `DISPUNIFORM` codes that indicate
#'   transfer out. Defaults to `2L`.
#'
#' @returns A lazy episode-level table.
#' @details
#' Step 2 of the core `easyNRD` pipeline. This function consolidates transfer-
#' linked contiguous discharges into episode-level records so
#' downstream phenotype and readmission logic operate on clinically coherent
#' units. Continuity is defined using transfer-out discharge coding
#' (`DISPUNIFORM` in `transfer_disp_codes`) and an admit-after-discharge gap of
#' at most one day. If available in the input, `SAMEDAYEVENT` is retained as
#' `Episode_SAMEDAYEVENT` so index definitions can follow HCUP guidance (for
#' example, requiring `Episode_SAMEDAYEVENT == 0L`).
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   episodes <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes()
#' }
#' }
nrd_build_episodes <- function(
  .data,
  transfer_disp_codes = 2L
) {
  .nrd_assert_lazy_duckdb(.data, arg = ".data")
  .data <- .nrd_standardize_names(.data)

  .nrd_assert_cols(
    .data,
    c(
      "YEAR", "NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT", "LOS",
      "DISPUNIFORM", "DMONTH", "DIED", "I10_DX1"
    )
  )

  row_level <- .data |>
    dplyr::group_by(YEAR, NRD_VISITLINK) |>
    nrd_window_order(NRD_DAYSTOEVENT, KEY_NRD) |>
    dplyr::mutate(
      .nrd_admit_day = as.numeric(NRD_DAYSTOEVENT),
      .nrd_los = as.numeric(LOS),
      .nrd_discharge_day_row = .nrd_admit_day + .nrd_los,
      .nrd_transfer_trigger = DISPUNIFORM %in% transfer_disp_codes,
      .nrd_prev_trigger = dplyr::lag(.nrd_transfer_trigger, default = FALSE),
      .nrd_prev_discharge = dplyr::lag(.nrd_discharge_day_row),
      .nrd_contiguous = dplyr::coalesce(.nrd_prev_trigger, FALSE) &
        !is.na(.nrd_admit_day) &
        !is.na(.nrd_prev_discharge) &
        dplyr::between(.nrd_admit_day - .nrd_prev_discharge, 0, 1),
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

  totchg_expr <- if ("TOTCHG" %in% colnames(row_keyed)) {
    rlang::expr(sum(as.numeric(TOTCHG), na.rm = TRUE))
  } else {
    rlang::expr(as.numeric(NA))
  }

  sameday_expr <- if ("SAMEDAYEVENT" %in% colnames(row_keyed)) {
    rlang::expr(
      dplyr::if_else(
        sum(dplyr::if_else(is.na(SAMEDAYEVENT), 0L, 1L), na.rm = TRUE) > 0L,
        max(SAMEDAYEVENT, na.rm = TRUE),
        NA_integer_
      )
    )
  } else {
    rlang::expr(NA_integer_)
  }

  row_keyed |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_ID) |>
    dplyr::summarise(
      Episode_Index_KEY_NRD = min(Episode_Index_KEY_NRD, na.rm = TRUE),
      Episode_KEY_NRD = max(Episode_KEY_NRD, na.rm = TRUE),
      Episode_DMONTH = max(Episode_DMONTH, na.rm = TRUE),
      Episode_Admission_Day = min(.nrd_admit_day, na.rm = TRUE),
      Episode_Discharge_Day = max(.nrd_discharge_day_row, na.rm = TRUE),
      Episode_LOS = sum(.nrd_los, na.rm = TRUE),
      Episode_TOTCHG = !!totchg_expr,
      Episode_DX10_Principal = max(.nrd_dx1_first, na.rm = TRUE),
      Episode_SAMEDAYEVENT = !!sameday_expr,
      DIED = max(DIED, na.rm = TRUE),
      Episode_N_Stays = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::left_join(
      .data |>
        dplyr::select(
          -dplyr::any_of(c(
            "DIED", "LOS", "TOTCHG", "DMONTH", "NRD_DAYSTOEVENT",
            "DISPUNIFORM"
          )),
          -dplyr::any_of(c("Episode_ID", "Episode_DX10_Principal"))
        ),
        by = c("YEAR", "NRD_VISITLINK", "Episode_Index_KEY_NRD" = "KEY_NRD")
    )
}
