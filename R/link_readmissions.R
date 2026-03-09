#' Link Index Episodes to Readmissions
#'
#' `nrd_link_readmissions()` links index discharges to the first qualifying
#' subsequent admission within a readmission window while preserving the complete
#' denominator.
#'
#' @param data Episode-level lazy table from `nrd_build_episodes()`.
#' @param index_condition Unquoted logical expression that defines index events.
#' @param readmit_condition Unquoted logical expression that defines qualifying
#'   readmission events.
#' @param window Integer readmission window in days. Defaults to `30L`.
#' @param readmit_vars Optional tidyselect specification for columns to pull from
#'   the linked readmission and append as wide `readmit_*` columns.
#'
#' @return A denominator-preserving lazy table with readmission linkage,
#'   `time_to_event`, and `outcome_status`.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   linked <- nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_link_readmissions(
#'       index_condition = Episode_DX10_Principal == "I214",
#'       readmit_condition = DIED == 0L,
#'       window = 30L,
#'       readmit_vars = c(Episode_TOTCHG, Episode_LOS, DIED)
#'     )
#' }
#' }
nrd_link_readmissions <- function(
  data,
  index_condition,
  readmit_condition,
  window = 30L,
  readmit_vars = NULL
) {
  data <- .nrd_standardize_names(data)

  .nrd_assert_cols(
    data,
    c(
      "YEAR", "NRD_VISITLINK", "Episode_ID", "Episode_KEY_NRD",
      "Episode_Admission_Day", "Episode_Discharge_Day", "Episode_DMONTH", "DIED"
    )
  )

  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)

  idx_quo <- rlang::enquo(index_condition)
  readm_quo <- rlang::enquo(readmit_condition)

  base <- data |>
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
    if (.nrd_should_sum_var(base, v)) {
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
