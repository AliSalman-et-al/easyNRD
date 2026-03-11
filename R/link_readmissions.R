#' Link Index Episodes to Readmissions
#'
#' `nrd_link_readmissions()` links index discharges to the first qualifying
#' subsequent admission within a readmission window while preserving the complete
#' denominator.
#'
#' @param .data Episode-level lazy table from [nrd_build_episodes()].
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
#' Step 3 of the core `easyNRD` pipeline. Under `censor_method = "drop_month"`,
#' late-year discharges that cannot guarantee full follow-up are retained in the
#' data but converted to `IndexEvent = 0L`, so they remain eligible as
#' readmission candidates for earlier index episodes. For standard readmission
#' analyses aligned to HCUP recommendations, include
#' `Episode_SAMEDAYEVENT == 0L` in `index_condition` when that field is
#' available from [nrd_build_episodes()].
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
#'   linked <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_link_readmissions(
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

  month_ends_non_leap <- c(31L, 59L, 90L, 120L, 151L, 181L, 212L, 243L, 273L, 304L, 334L, 365L)
  month_ends_leap <- c(31L, 60L, 91L, 121L, 152L, 182L, 213L, 244L, 274L, 305L, 335L, 366L)

  max_month_for_window <- function(month_ends, year_days, window_days) {
    allowable <- which(month_ends <= (year_days - window_days))
    if (length(allowable) == 0) 0L else as.integer(max(allowable))
  }

  last_allowable_month_non_leap <- max_month_for_window(month_ends_non_leap, 365L, window)
  last_allowable_month_leap <- max_month_for_window(month_ends_leap, 366L, window)

  base <- .data |>
    dplyr::mutate(IndexEvent = dplyr::if_else(!!idx_quo, 1L, 0L, missing = 0L)) |>
    .nrd_add_year_end_censoring(window = window, censor_method = censor_method) |>
    dplyr::mutate(
      .nrd_is_leap = (YEAR %% 400L == 0L) | (YEAR %% 4L == 0L & YEAR %% 100L != 0L),
      .nrd_last_allowable_month = dplyr::if_else(
        .nrd_is_leap,
        last_allowable_month_leap,
        last_allowable_month_non_leap
      ),
      .nrd_followup_complete_strict = !is.na(Episode_DMONTH) & Episode_DMONTH <= .nrd_last_allowable_month,
      IndexEvent = dplyr::if_else(
        !allow_censored_followup &
          IndexEvent == 1L &
          dplyr::coalesce(.nrd_followup_complete_strict, FALSE) == FALSE,
        0L,
        IndexEvent
      )
    )

  readmit_names <- .nrd_resolve_readmit_vars(base, {{ readmit_vars }})
  readmit_double_map <- .nrd_double_map(base, readmit_names)

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
    dplyr::slice_min(order_by = .nrd_gap_days, n = 1L, with_ties = TRUE)

  summarise_exprs <- list(
    first_readmit_gap = rlang::expr(min(.nrd_gap_days, na.rm = TRUE)),
    Episode_KEY_NRD_cand = rlang::expr(max(Episode_KEY_NRD_cand, na.rm = TRUE))
  )

  for (v in readmit_names) {
    cand_sym <- rlang::sym(v)
    out_nm <- paste0("readmit_", v)
    if (.nrd_should_sum_var(base, v, double_map = readmit_double_map)) {
      summarise_exprs[[out_nm]] <- rlang::expr(sum(!!cand_sym, na.rm = TRUE))
    } else {
      summarise_exprs[[out_nm]] <- rlang::expr(max(!!cand_sym, na.rm = TRUE))
    }
  }

  first_map <- first_candidates |>
    dplyr::group_by(YEAR, NRD_VISITLINK, Episode_KEY_NRD_idx) |>
    dplyr::summarise(!!!summarise_exprs, .groups = "drop") |>
    dplyr::rename(Episode_KEY_NRD = Episode_KEY_NRD_idx)

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
        ".nrd_followup_complete", ".nrd_is_leap",
        ".nrd_last_allowable_month", ".nrd_followup_complete_strict"
      ))
    )
}
