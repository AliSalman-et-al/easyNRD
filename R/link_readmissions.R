# Validate the requested readmission window before linking.
.nrd_validate_window <- function(window) {
  if (!is.numeric(window) || length(window) != 1 || is.na(window) || window < 1 || window != as.integer(window)) {
    rlang::abort("`window` must be a single positive integer.")
  }

  as.integer(window)
}

# Build a discharge-day expression with or without prior `nrd_prepare()` output.
.nrd_discharge_day_expr <- function(data) {
  if ("Discharge_Day" %in% colnames(data)) {
    return(rlang::expr(Discharge_Day))
  }

  rlang::expr(NRD_DAYSTOEVENT + LOS)
}

# Materialize the annotated base table before the self-join.
.nrd_checkpoint_link_base <- function(data, index_condition, window) {
  censor_limit <- 12L - ceiling(window / 30L)
  discharge_day <- .nrd_discharge_day_expr(data)

  data |>
    dplyr::mutate(
      is_index_eligible = DMONTH <= censor_limit,
      .nrd_died_at_index = dplyr::coalesce(!!index_condition, FALSE) & is_index_eligible & DIED == 1L,
      IndexEvent = dplyr::if_else(
        is_index_eligible & DIED == 0L & dplyr::coalesce(!!index_condition, FALSE),
        1L,
        0L,
        missing = 0L
      ),
      .nrd_censor_day = !!discharge_day
    ) |>
    dplyr::compute()
}

# Create the index pool with only the columns needed for the self-join.
.nrd_index_pool <- function(data) {
  data |>
    dplyr::filter(IndexEvent == 1L) |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      KEY_NRD_idx = KEY_NRD,
      DTE_idx = NRD_DAYSTOEVENT,
      LOS_idx = LOS
    )
}

# Create readmission transmute expressions for requested carry-through columns.
.nrd_readmit_exprs <- function(readmit_vars) {
  stats::setNames(lapply(readmit_vars, rlang::sym), readmit_vars)
}

# Create the candidate pool with only the columns needed after filtering.
.nrd_candidate_pool <- function(data, readmit_condition, readmit_vars) {
  data |>
    dplyr::filter(!!readmit_condition) |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      KEY_NRD_cand = KEY_NRD,
      DTE_cand = NRD_DAYSTOEVENT,
      !!!.nrd_readmit_exprs(readmit_vars)
    )
}

# Keep the first qualifying readmission for each index discharge.
.nrd_first_readmission <- function(index_pool, candidate_pool, window) {
  index_pool |>
    dplyr::inner_join(
      candidate_pool,
      by = c("YEAR", "NRD_VISITLINK"),
      relationship = "many-to-many"
    ) |>
    dplyr::filter(KEY_NRD_cand != KEY_NRD_idx) |>
    dplyr::mutate(gap = DTE_cand - DTE_idx - LOS_idx) |>
    dplyr::filter(gap >= 1L, gap <= window) |>
    dplyr::group_by(YEAR, NRD_VISITLINK, KEY_NRD_idx) |>
    dplyr::slice_min(order_by = gap, n = 1L, with_ties = TRUE) |>
    dplyr::slice_min(order_by = DTE_cand, n = 1L, with_ties = TRUE) |>
    dplyr::slice_min(order_by = KEY_NRD_cand, n = 1L, with_ties = FALSE) |>
    dplyr::ungroup()
}

# Rename requested readmission columns with the required public prefix.
.nrd_readmit_output_exprs <- function(readmit_vars) {
  if (length(readmit_vars) == 0) {
    return(list())
  }

  stats::setNames(lapply(readmit_vars, rlang::sym), paste0("readmit_", readmit_vars))
}

# Add public readmission outcomes after joining back to the denominator.
.nrd_finalize_linkage <- function(data, first_map, window) {
  data |>
    dplyr::left_join(
      first_map,
      by = c("YEAR", "NRD_VISITLINK", "KEY_NRD" = "KEY_NRD_idx")
    ) |>
    dplyr::mutate(
      time_to_event = dplyr::case_when(
        .nrd_died_at_index ~ 0,
        IndexEvent == 1L & !is.na(gap) ~ as.numeric(gap),
        IndexEvent == 1L ~ as.numeric(
          dplyr::if_else(
            (365L - .nrd_censor_day) < 0L,
            0L,
            dplyr::if_else((365L - .nrd_censor_day) < window, 365L - .nrd_censor_day, window)
          )
        ),
        TRUE ~ NA_real_
      ),
      outcome_status = dplyr::case_when(
        .nrd_died_at_index ~ "Died at Index",
        IndexEvent == 1L & !is.na(gap) ~ "Readmitted",
        IndexEvent == 1L ~ "Censored",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(-is_index_eligible, -.nrd_died_at_index, -.nrd_censor_day, -gap)
}

#' Link first qualifying readmissions back to eligible index discharges
#'
#' `nrd_link_readmissions()` links the first qualifying readmission within a
#' fixed window while preserving the full denominator.
#'
#' @param data A DuckDB-backed lazy table.
#' @param index_condition Unquoted logical expression defining candidate index rows.
#' @param readmit_condition Unquoted logical expression defining qualifying readmissions.
#' @param window Integer readmission window in days. Defaults to `30L`.
#' @param readmit_vars Optional tidyselect specification for columns to append
#'   from the linked readmission with a `readmit_` prefix.
#'
#' @returns A DuckDB-backed `tbl_lazy` with linkage results added.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/nrd.parquet")
#'   linked <- nrd_link_readmissions(
#'     data,
#'     index_condition = is_index == 1L,
#'     readmit_condition = TRUE,
#'     readmit_vars = c(KEY_NRD, LOS)
#'   )
#' }
nrd_link_readmissions <- function(
  data,
  index_condition,
  readmit_condition,
  window = 30L,
  readmit_vars = NULL
) {
  .nrd_assert_duckdb_lazy(data)
  .nrd_assert_cols(
    data,
    c("YEAR", "NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT", "LOS", "DMONTH", "DIED")
  )

  window <- .nrd_validate_window(window)
  index_condition <- rlang::enquo(index_condition)
  readmit_condition <- rlang::enquo(readmit_condition)

  # Step 1-3: annotate index eligibility and checkpoint the base table.
  base <- .nrd_checkpoint_link_base(data, index_condition = index_condition, window = window)

  # Step 4-5: build the index and candidate pools with early projection.
  readmit_vars <- .nrd_resolve_readmit_vars(base, {{ readmit_vars }})
  index_pool <- .nrd_index_pool(base)
  candidate_pool <- .nrd_candidate_pool(base, readmit_condition = readmit_condition, readmit_vars = readmit_vars)

  # Step 6-7: self-join within patient-year scope and keep the first readmission.
  first_map <- .nrd_first_readmission(index_pool, candidate_pool, window = window) |>
    dplyr::transmute(
      YEAR,
      NRD_VISITLINK,
      KEY_NRD_idx,
      gap,
      !!!.nrd_readmit_output_exprs(readmit_vars)
    )

  # Step 8: join the first-readmission map back to the full denominator.
  .nrd_finalize_linkage(base, first_map = first_map, window = window)
}
