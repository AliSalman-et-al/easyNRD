#' Link Index Episodes to Readmissions
#'
#' `nrd_link_readmissions()` links index discharges to the first qualifying
#' subsequent admission within a readmission window while preserving the complete
#' denominator.
#'
#' @param .data Episode-level lazy table.
#' @param index_condition Unquoted logical expression that defines index events.
#' @param readmit_condition Unquoted logical expression that defines qualifying
#'   readmission events.
#' @param window Integer readmission window in days. Defaults to `30L`.
#' @param censor_method Year-end censoring strategy. `"drop_month"` (default)
#'   marks late-year discharges as non-index events to guarantee complete
#'   follow-up for the selected window while preserving them as candidate
#'   readmissions. `"mid_month"` uses a mid-month approximation for censoring
#'   time.
#' @param readmit_vars Optional tidyselect specification for columns to pull from
#'   the linked readmission and append as wide `readmit_*` columns.
#' @param allow_censored_followup Logical flag that controls treatment of index
#'   stays discharged too late in the year to complete full same-year follow-up.
#'   Defaults to `FALSE` for strict HCUP-style binary readmission denominators;
#'   these rows are retained but converted to `IndexEvent = 0L`. Set to `TRUE`
#'   to retain such rows as index stays with administrative right-censoring at
#'   calendar year end.
#'
#' @returns A denominator-preserving lazy table with readmission linkage,
#'   `time_to_event`, and `outcome_status`.
#' @details
#' Step 3 of the legacy prototype pipeline. Under `censor_method = "drop_month"`,
#' late-year discharges that cannot guarantee full follow-up are retained in the
#' data but converted to `IndexEvent = 0L`, so they remain eligible as
#' readmission candidates for earlier index episodes. For standard readmission
#' analyses aligned to HCUP recommendations, include
#' `Episode_SAMEDAYEVENT == 0L` in `index_condition` when that field is
#' available from an upstream episode-building step.
#'
#' For standard readmission
#' rate denominators, exclude index stays with `outcome_status == "Died at
#' Index"` (typically equivalent to requiring `DIED == 0L` at index discharge).
#' Those rows can be retained when fitting competing-risks or multi-state
#' survival models. The output preserves `DIED` and linked event timing fields,
#' so it can be passed directly to competing-risk modeling workflows in
#' `survival`.
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   linked <- nrd_link_readmissions(
#'     nrd_ingest("/path/to/nrd.parquet"),
#'       index_condition = Episode_DX10_Principal == "I214" & Episode_SAMEDAYEVENT == 0L,
#'       readmit_condition = DIED == 0L,
#'       window = 30L,
#'       readmit_vars = c(Episode_TOTCHG, Episode_LOS, DIED)
#'     )
#' }
#' }
nrd_link_readmissions <- function(
  .data,
  index_condition,
  readmit_condition,
  window = 30L,
  censor_method = c("drop_month", "mid_month"),
  readmit_vars = NULL,
  allow_censored_followup = FALSE
) {
  .nrd_assert_lazy_duckdb(.data, arg = ".data")
  .data <- .nrd_standardize_names(.data)

  .nrd_assert_cols(
    .data,
    c(
      "YEAR", "NRD_VISITLINK", "Episode_ID", "Episode_KEY_NRD",
      "Episode_Admission_Day", "Episode_Discharge_Day", "Episode_DMONTH", "DIED"
    )
  )

  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)
  censor_method <- rlang::arg_match(censor_method)
  if (!is.logical(allow_censored_followup) || length(allow_censored_followup) != 1 || is.na(allow_censored_followup)) {
    rlang::abort("`allow_censored_followup` must be TRUE or FALSE.")
  }

  idx_quo <- rlang::enquo(index_condition)
  readm_quo <- rlang::enquo(readmit_condition)

  base <- .data |>
    dplyr::mutate(IndexEvent = dplyr::if_else(!!idx_quo, 1L, 0L, missing = 0L)) |>
    .nrd_add_year_end_censoring(window = window, censor_method = censor_method) |>
    dplyr::mutate(
      IndexEvent = dplyr::if_else(
        !allow_censored_followup &
          IndexEvent == 1L &
          dplyr::coalesce(.nrd_followup_complete, FALSE) == FALSE,
        0L,
        IndexEvent
      )
    )

  readmit_names <- .nrd_resolve_readmit_vars(base, {{ readmit_vars }})

  readmit_condition_vars <- intersect(
    colnames(base),
    unique(all.vars(rlang::get_expr(readm_quo)))
  )

  index_pool <- base |>
    dplyr::filter(
      IndexEvent == 1L,
      !is.na(NRD_VISITLINK),
      !is.na(Episode_Discharge_Day)
    ) |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      Episode_ID_idx = Episode_ID,
      Episode_KEY_NRD_idx = Episode_KEY_NRD,
      Episode_Discharge_Day_idx = Episode_Discharge_Day
    )

  rhs_cols <- unique(c(
    "YEAR", "NRD_VISITLINK", "Episode_ID", "Episode_KEY_NRD",
    "Episode_Admission_Day", "Episode_Discharge_Day",
    readmit_names,
    readmit_condition_vars
  ))

  candidate_pool <- base |>
    dplyr::select(dplyr::any_of(rhs_cols)) |>
    dplyr::filter(
      !is.na(NRD_VISITLINK),
      !is.na(Episode_Admission_Day),
      !!readm_quo
    ) |>
    dplyr::rename(
      Episode_ID_cand = Episode_ID,
      Episode_KEY_NRD_cand = Episode_KEY_NRD,
      Episode_Admission_Day_cand = Episode_Admission_Day,
      Episode_Discharge_Day_cand = Episode_Discharge_Day
    )

  paired <- index_pool |>
    dplyr::inner_join(
      candidate_pool,
      by = c("YEAR", "NRD_VISITLINK"),
      relationship = "many-to-many"
    ) |>
    dplyr::filter(Episode_ID_idx != Episode_ID_cand) |>
    dplyr::mutate(.nrd_gap_days = Episode_Admission_Day_cand - Episode_Discharge_Day_idx) |>
    dplyr::filter(dplyr::between(.nrd_gap_days, 1L, window))

  first_candidates <- paired |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_KEY_NRD_idx) |>
    dplyr::filter(.nrd_gap_days == min(.nrd_gap_days, na.rm = TRUE)) |>
    dplyr::filter(Episode_Admission_Day_cand == min(Episode_Admission_Day_cand, na.rm = TRUE)) |>
    dplyr::slice_min(order_by = Episode_KEY_NRD_cand, n = 1L, with_ties = FALSE) |>
    dplyr::ungroup()

  rhs_renamed <- c(
    Episode_ID = "Episode_ID_cand",
    Episode_KEY_NRD = "Episode_KEY_NRD_cand",
    Episode_Admission_Day = "Episode_Admission_Day_cand",
    Episode_Discharge_Day = "Episode_Discharge_Day_cand"
  )
  readmit_rhs_names <- ifelse(
    readmit_names %in% names(rhs_renamed),
    unname(rhs_renamed[readmit_names]),
    readmit_names
  )

  readmit_exprs <- list()
  if (length(readmit_names) > 0) {
    readmit_exprs <- stats::setNames(
      lapply(readmit_rhs_names, rlang::sym),
      paste0("readmit_", readmit_names)
    )
  }

  first_map <- first_candidates |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      Episode_KEY_NRD = Episode_KEY_NRD_idx,
      first_readmit_gap = .nrd_gap_days,
      Episode_KEY_NRD_cand,
      !!!readmit_exprs
    )

  base |>
    dplyr::left_join(first_map, by = c("YEAR", "NRD_VISITLINK", "Episode_KEY_NRD")) |>
    dplyr::mutate(
      time_to_event = dplyr::case_when(
        IndexEvent == 0L ~ NA_real_,
        DIED == 1L ~ 0,
        !is.na(first_readmit_gap) ~ as.numeric(first_readmit_gap),
        TRUE ~ dplyr::if_else(
          is.na(Days_to_End_of_Year),
          NA_real_,
          as.numeric(dplyr::if_else(Days_to_End_of_Year < window, Days_to_End_of_Year, window))
        )
      ),
      outcome_status = dplyr::case_when(
        IndexEvent == 0L ~ NA_character_,
        DIED == 1L ~ "Died at Index",
        !is.na(first_readmit_gap) ~ "Readmitted",
        TRUE ~ "Censored"
      )
    ) |>
    dplyr::select(
      -Episode_KEY_NRD_cand,
      -first_readmit_gap,
      -dplyr::any_of(c(
        ".nrd_followup_complete"
      ))
    )
}
