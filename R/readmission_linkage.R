.nrd_outcome_required_cols <- c(
  "YEAR", "NRD_VISITLINK", "Episode_ID", "Episode_KEY_NRD",
  "Episode_Admission_Day", "Episode_Discharge_Day", "Episode_DMONTH", "DIED"
)

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

.nrd_first_readmission_map <- function(.data, index_condition, window) {
  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)
  idx_quo <- rlang::enquo(index_condition)

  base <- .data |>
    dplyr::mutate(IndexEvent = dplyr::if_else(!!idx_quo, 1L, 0L, missing = 0L))

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

  candidate_pool <- base |>
    dplyr::filter(
      !is.na(NRD_VISITLINK),
      !is.na(Episode_Admission_Day)
    ) |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      Episode_ID_cand = Episode_ID,
      Episode_KEY_NRD_cand = Episode_KEY_NRD,
      Episode_Admission_Day_cand = Episode_Admission_Day
    )

  paired <- index_pool |>
    dplyr::inner_join(
      candidate_pool,
      by = c("YEAR", "NRD_VISITLINK"),
      relationship = "many-to-many"
    ) |>
    dplyr::filter(Episode_ID_cand != Episode_ID_idx) |>
    dplyr::mutate(.nrd_gap_days = Episode_Admission_Day_cand - Episode_Discharge_Day_idx) |>
    dplyr::filter(dplyr::between(.nrd_gap_days, 1L, window))

  first_map <- paired |>
    dplyr::group_by(YEAR, Episode_KEY_NRD_idx) |>
    nrd_window_order(.nrd_gap_days, Episode_Admission_Day_cand, Episode_KEY_NRD_cand) |>
    dplyr::mutate(.nrd_rank = dplyr::row_number()) |>
    dplyr::filter(.nrd_rank == 1L) |>
    dplyr::transmute(
      YEAR,
      Episode_KEY_NRD_idx,
      Episode_KEY_NRD_cand,
      first_readmit_gap = .nrd_gap_days
    ) |>
    dplyr::ungroup()

  list(base = base, first_map = first_map)
}

#' Track Competing-Risk Outcomes from Episode-Level NRD Data
#'
#' Creates denominator-preserving survival variables for competing-risk analyses
#' after episode assembly.
#'
#' @details
#' This function is purpose-built for survival modeling workflows where the
#' nationwide denominator must remain intact for survey-based variance
#' estimation. No global exclusions are applied.
#'
#' Methodological sequence:
#'
#' - evaluate `index_condition` to create `IndexEvent`
#' - compute right-censoring support (`Days_to_End_of_Year`) using
#'   `Episode_DMONTH` and a 15th-of-month discharge approximation
#' - identify first valid readmission within `window` days by many-to-many
#'   self-join on `YEAR` + `NRD_VISITLINK`, linking index discharge to candidate
#'   admission
#' - generate compact survival outputs only (no wide candidate payload)
#'
#' HCUP-NRD linkage is constrained to a single calendar year; this function
#' respects that architecture by joining only within `YEAR`.
#'
#' `outcome_status` is returned as a character column to ensure reliable SQL
#' translation on lazy backends. After `collect()`, users should cast to factor
#' with levels:
#'
#' 1. `"Died at Index"`
#' 2. `"Readmitted"`
#' 3. `"Censored"`
#'
#' @param .data Episode-level lazy table from [nrd_assemble_episodes()].
#' @param index_condition Data-masked expression defining index episodes.
#' @param window Integer readmission window in days. Defaults to `30L`.
#'
#' @return Full input table with appended `IndexEvent`, `Days_to_End_of_Year`,
#'   `time_to_event`, and `outcome_status` (character).
#' @export
#'
#' @examples
#' \donttest{
#' episodes <- tibble::tibble(
#'   YEAR = c(2019L, 2019L),
#'   NRD_VISITLINK = c(1L, 1L),
#'   Episode_ID = c(1L, 2L),
#'   Episode_KEY_NRD = c(1001L, 1002L),
#'   Episode_Admission_Day = c(10, 25),
#'   Episode_Discharge_Day = c(12, 30),
#'   Episode_DMONTH = c(1L, 1L),
#'   DIED = c(0L, 0L)
#' )
#' outcomes <- episodes |>
#'   nrd_track_outcomes(index_condition = Episode_ID >= 1, window = 30L)
#' }
nrd_track_outcomes <- function(.data, index_condition, window = 30L) {
  .nrd_assert_cols(.data, .nrd_outcome_required_cols)

  linked <- .nrd_first_readmission_map(
    .data = .data,
    index_condition = {{ index_condition }},
    window = window
  )

  out <- linked$base |>
    .nrd_add_year_end_censoring() |>
    dplyr::left_join(
      linked$first_map |>
        dplyr::select(YEAR, Episode_KEY_NRD = Episode_KEY_NRD_idx, first_readmit_gap),
      by = c("YEAR", "Episode_KEY_NRD")
    ) |>
    dplyr::mutate(
      time_to_event = dplyr::case_when(
        IndexEvent == 0L ~ NA_real_,
        DIED == 1L ~ 0,
        !is.na(first_readmit_gap) ~ as.numeric(first_readmit_gap),
        TRUE ~ dplyr::if_else(
          is.na(Days_to_End_of_Year),
          NA_real_,
          as.numeric(dplyr::if_else(Days_to_End_of_Year < as.integer(window), Days_to_End_of_Year, as.integer(window)))
        )
      ),
      outcome_status = dplyr::case_when(
        IndexEvent == 0L ~ NA_character_,
        DIED == 1L ~ "Died at Index",
        !is.na(first_readmit_gap) ~ "Readmitted",
        TRUE ~ "Censored"
      )
    ) |>
    dplyr::select(-first_readmit_gap)

  out
}

#' Flag First Readmission Episodes for Secondary Clinical Profiling
#'
#' Identifies first qualifying readmission episodes linked to a target index
#' cohort and appends extraction flags to the full denominator-preserving table.
#'
#' @details
#' This module intentionally separates clinical profiling from survival outcome
#' construction. It performs the same temporal linkage used in
#' [nrd_track_outcomes()] and marks only episodes that are true first
#' readmissions for the index cohort.
#'
#' Returned flags:
#'
#' - `Is_Readmission_Event`: row is a first readmission episode
#' - `Linked_To_Target_Index`: row belongs to the first-readmission pool linked
#'   to the index cohort defined by `index_condition`
#'
#' Because the full lazy table is returned without dropping rows, users can
#' construct complex survey designs first, then subset analytically.
#'
#' @param .data Episode-level lazy table from [nrd_assemble_episodes()].
#' @param index_condition Data-masked expression defining index episodes.
#' @param window Integer readmission window in days. Defaults to `30L`.
#'
#' @return Full input table with appended integer flags
#'   `Is_Readmission_Event` and `Linked_To_Target_Index`.
#' @export
#'
#' @examples
#' \donttest{
#' episodes <- tibble::tibble(
#'   YEAR = c(2019L, 2019L),
#'   NRD_VISITLINK = c(1L, 1L),
#'   Episode_ID = c(1L, 2L),
#'   Episode_KEY_NRD = c(1001L, 1002L),
#'   Episode_Admission_Day = c(10, 25),
#'   Episode_Discharge_Day = c(12, 30),
#'   Episode_DMONTH = c(1L, 1L),
#'   DIED = c(0L, 0L)
#' )
#' flagged <- episodes |>
#'   nrd_extract_readmissions(index_condition = Episode_ID >= 1, window = 30L)
#' }
nrd_extract_readmissions <- function(.data, index_condition, window = 30L) {
  .nrd_assert_cols(.data, .nrd_outcome_required_cols)

  linked <- .nrd_first_readmission_map(
    .data = .data,
    index_condition = {{ index_condition }},
    window = window
  )

  readmit_keys <- linked$first_map |>
    dplyr::transmute(
      YEAR,
      Episode_KEY_NRD = Episode_KEY_NRD_cand,
      Is_Readmission_Event = 1L,
      Linked_To_Target_Index = 1L
    ) |>
    dplyr::distinct(YEAR, Episode_KEY_NRD, Is_Readmission_Event, Linked_To_Target_Index)

  .data |>
    dplyr::left_join(readmit_keys, by = c("YEAR", "Episode_KEY_NRD")) |>
    dplyr::mutate(
      Is_Readmission_Event = dplyr::coalesce(Is_Readmission_Event, 0L),
      Linked_To_Target_Index = dplyr::coalesce(Linked_To_Target_Index, 0L)
    )
}
