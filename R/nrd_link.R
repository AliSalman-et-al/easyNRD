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

.nrd_is_sum_var <- function(.data, var) {
  default_sum_vars <- c("Episode_TOTCHG", "TOTCHG")
  if (var %in% default_sum_vars) {
    return(TRUE)
  }

  if (!inherits(.data, "tbl_lazy") && var %in% colnames(.data)) {
    col <- .data[[var]]
    if (is.double(col)) {
      return(TRUE)
    }
  }

  FALSE
}

#' Link First Readmissions and Survival Outcomes
#'
#' `nrd_link()` adds denominator-preserving outcome variables to an episode-level
#' NRD table and optionally appends wide readmission attributes from the first
#' linked readmission episode.
#'
#' @details
#' This function preserves the complete episode denominator and does not drop
#' rows. It evaluates `index_condition` to create `IndexEvent`, computes
#' `Days_to_End_of_Year` using the HCUP-compatible month midpoint approximation,
#' and links index discharge to candidate admissions within the same
#' `YEAR` + `NRD_VISITLINK` trajectory.
#'
#' First qualifying readmissions are selected within `window` days and used to
#' generate:
#'
#' - `time_to_event`
#' - `outcome_status` (`"Died at Index"`, `"Readmitted"`, `"Censored"`)
#'
#' `outcome_status` is returned as character for lazy backend safety.
#'
#' If `readmit_vars` are provided, tied minimum-gap readmissions are aggregated
#' and joined back to index rows as `readmit_*` columns.
#'
#' @param .data Episode-level table, typically from [nrd_read()].
#' @param index_condition Data-masked expression for index episodes.
#' @param readmit_condition Data-masked expression for readmission candidates.
#' @param window Integer readmission window in days. Defaults to `30L`.
#' @param readmit_vars Tidyselect-style expression of candidate columns to append
#'   as wide `readmit_*` fields.
#'
#' @return Full denominator-preserving lazy table with outcome columns and
#'   optional `readmit_*` columns.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   linked <- nrd_read("/path/to/nrd/parquet") |>
#'     dplyr::mutate(target_index = Episode_DX10_Principal == "A419") |>
#'     nrd_link(
#'       index_condition = target_index,
#'       readmit_condition = DIED == 0L,
#'       window = 30L,
#'       readmit_vars = c(Episode_TOTCHG, DIED)
#'     )
#' }
#' }
nrd_link <- function(
  .data,
  index_condition,
  readmit_condition,
  window = 30L,
  readmit_vars = NULL
) {
  .nrd_assert_cols(
    .data,
    c(
      "YEAR", "NRD_VISITLINK", "Episode_ID", "Episode_KEY_NRD",
      "Episode_Admission_Day", "Episode_Discharge_Day", "Episode_DMONTH", "DIED"
    )
  )

  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)

  idx_quo <- rlang::enquo(index_condition)
  readm_quo <- rlang::enquo(readmit_condition)

  base <- .data |>
    dplyr::mutate(IndexEvent = dplyr::if_else(!!idx_quo, 1L, 0L, missing = 0L)) |>
    .nrd_add_year_end_censoring()

  readmit_names <- .nrd_resolve_readmit_vars(base, {{ readmit_vars }})

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
      Episode_Discharge_Day_idx = Episode_Discharge_Day,
      DIED_idx = DIED,
      Days_to_End_of_Year_idx = Days_to_End_of_Year
    )

  candidate_cols <- unique(c(
    "Episode_ID", "Episode_KEY_NRD", "Episode_Admission_Day", readmit_names
  ))

  candidate_pool <- base |>
    dplyr::filter(
      !is.na(NRD_VISITLINK),
      !is.na(Episode_Admission_Day),
      !!readm_quo
    ) |>
    dplyr::select(YEAR, NRD_VISITLINK, dplyr::any_of(candidate_cols)) |>
    dplyr::rename(
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
    dplyr::filter(Episode_ID_idx != Episode_ID_cand) |>
    dplyr::mutate(.nrd_gap_days = Episode_Admission_Day_cand - Episode_Discharge_Day_idx) |>
    dplyr::filter(dplyr::between(.nrd_gap_days, 1L, window))

  first_candidates <- paired |>
    dplyr::group_by(YEAR, Episode_KEY_NRD_idx) |>
    dplyr::filter(.nrd_gap_days == min(.nrd_gap_days, na.rm = TRUE))

  summarise_exprs <- list(
    first_readmit_gap = rlang::expr(min(.nrd_gap_days, na.rm = TRUE)),
    Episode_KEY_NRD_cand = rlang::expr(max(Episode_KEY_NRD_cand, na.rm = TRUE))
  )

  for (v in readmit_names) {
    cand_sym <- rlang::sym(v)
    out_nm <- paste0("readmit_", v)
    if (.nrd_is_sum_var(base, v)) {
      summarise_exprs[[out_nm]] <- rlang::expr(sum(!!cand_sym, na.rm = TRUE))
    } else {
      summarise_exprs[[out_nm]] <- rlang::expr(max(!!cand_sym, na.rm = TRUE))
    }
  }

  first_map <- first_candidates |>
    dplyr::group_by(YEAR, Episode_KEY_NRD_idx) |>
    dplyr::summarise(!!!summarise_exprs, .groups = "drop") |>
    dplyr::rename(Episode_KEY_NRD = Episode_KEY_NRD_idx)

  base |>
    dplyr::left_join(first_map, by = c("YEAR", "Episode_KEY_NRD")) |>
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
    dplyr::select(-Episode_KEY_NRD_cand, -first_readmit_gap)
}
