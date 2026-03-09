#' Calculate HCUP-NRD Readmission Metrics for Index Admissions
#'
#' Derives index-event and first-readmission indicators on a lazy NRD table
#' while preserving the original row denominator.
#'
#' @description
#' `calculate_readmission()` identifies eligible index admissions, searches for
#' subsequent qualifying admissions for the same patient, and writes
#' readmission-related outputs back onto the original table.
#'
#' The function is designed for NRD workflows using lazy backends
#' (DuckDB/Arrow via `dplyr`), and supports repeated calls with different
#' windows or cohort definitions via `suffix`.
#'
#' @details
#' Methodological behavior:
#' \itemize{
#'   \item \strong{Denominator preservation}: no global row-level exclusion is
#'   applied to the incoming table. Rows with incomplete timing fields remain in
#'   the output and receive `0`/`NA` readmission fields as appropriate.
#'
#'   \item \strong{Pool-specific timing filters}: missing `LOS` or
#'   `NRD_DAYSTOEVENT` are filtered only when constructing index and candidate
#'   pools.
#'
#'   \item \strong{Verified linkage and survival for index stays}: both pools
#'   require non-missing `NRD_VISITLINK`, and index stays are restricted to
#'   `DIED == 0` to enforce physiologic eligibility for subsequent readmission.
#'
#'   \item \strong{Month-based full follow-up censoring}: because NRD time to
#'   event is anchored to a synthetic patient-specific date, full follow-up is
#'   approximated using discharge month. Index eligibility requires
#'   `DMONTH <= 12 - ceiling(window / 30)`.
#'
#'   \item \strong{Candidate transfer handling}: same-day event rows are
#'   retained in the candidate pool to preserve clinically contiguous
#'   inter-facility readmission episodes. Transfer-chain protection is enforced
#'   by requiring strictly positive `gap_days` (`1..window`).
#'
#'   \item \strong{First readmission logic}: among qualifying candidates, all
#'   events tied at the minimum gap are retained for each index stay.
#'
#'   \item \strong{Episode-level metric aggregation}: tied first-gap events are
#'   collapsed to one index-row writeback using summed charges and maximal
#'   mortality indicator, with explicit `NA` propagation when all tied source
#'   values are missing for a metric.
#'
#'   \item \strong{Join safety}: first-readmission row keys are deduplicated
#'   before joining back to prevent many-to-many expansion when one candidate
#'   stay can map to multiple prior indexes.
#' }
#'
#' Required input columns:
#' \itemize{
#'   \item `YEAR`
#'   \item `DMONTH`
#'   \item `NRD_VISITLINK`
#'   \item `KEY_NRD`
#'   \item `NRD_DAYSTOEVENT`
#'   \item `LOS`
#'   \item `TOTCHG`
#'   \item `DIED`
#' }
#'
#' @param .data A lazy table (`tbl_lazy` or Arrow tabular object) containing NRD
#' hospitalization rows.
#' @param index_filter A data-masked expression defining index-eligible
#' admissions (e.g., age, diagnosis, elective status).
#' @param readmit_filter A data-masked expression defining candidate
#' readmission eligibility. Defaults to `TRUE` (all-cause candidates, subject to
#' built-in candidate timing restrictions).
#' @param window Integer follow-up window in days (e.g., `30L`, `60L`, `90L`).
#' Must be `>= 1`.
#' @param suffix Optional character scalar appended to output column names. If
#' provided without a leading underscore, `"_"` is prepended automatically.
#'
#' @return
#' The original lazy table with these added columns (optionally suffixed):
#' \describe{
#'   \item{`IndexEvent`}{Integer `0/1`: row qualifies as an index event under
#'   `index_filter` and full-follow-up requirements.}
#'   \item{`Readmit`}{Integer `0/1`: index row has a qualifying first
#'   readmission within `window`.}
#'   \item{`DaysToReadmit`}{Numeric gap in days from pseudo-discharge to first
#'   qualifying readmission; `NA` if none.}
#'   \item{`TotalReadmissions`}{Numeric count of distinct qualifying gap-days
#'   within `window` for the index stay.}
#'   \item{`ReadmitCHG`}{Total charge across tied first-gap candidate rows for
#'   the qualifying readmission episode; `NA` if none or if all tied charge
#'   values are missing.}
#'   \item{`ReadmitDIED`}{Maximum in-hospital death flag across tied first-gap
#'   candidate rows; `NA` if none or if all tied mortality values are missing.}
#'   \item{`ReadmissionRow`}{Integer `0/1`: row is itself selected as a first
#'   qualifying readmission for at least one index stay.}
#' }
#'
#' @examples
#' \dontrun{
#' nrd_tbl %>%
#'   calculate_readmission(
#'     index_filter = AGE >= 18 & ELECTIVE == "Non-elective",
#'     readmit_filter = TRUE,
#'     window = 30
#'   )
#'
#' nrd_tbl %>%
#'   calculate_readmission(
#'     index_filter = AGE >= 18 & ELECTIVE == "Non-elective",
#'     readmit_filter = isTRAUMA == "No",
#'     window = 90,
#'     suffix = "90d"
#'   )
#' }
#'
#' @export

calculate_readmission <- function(
  .data,
  index_filter,
  readmit_filter = TRUE,
  window = 30L,
  suffix = NULL
) {
  if (!inherits(.data, "tbl_lazy") && !inherits(.data, "ArrowTabular")) {
    stop(".data must be a lazy dplyr or Arrow table.")
  }

  stopifnot(is.numeric(window), length(window) == 1, window >= 1)
  window <- as.integer(window)
  last_allowable_month <- as.integer(12L - ceiling(window / 30))

  if (is.null(suffix)) {
    suffix <- ""
  } else {
    stopifnot(is.character(suffix), length(suffix) == 1)
    if (!startsWith(suffix, "_")) suffix <- paste0("_", suffix)
  }

  quo_index <- rlang::enquo(index_filter)
  quo_readmit <- rlang::enquo(readmit_filter)

  required_cols <- c(
    "YEAR", "DMONTH", "NRD_VISITLINK", "KEY_NRD", "NRD_DAYSTOEVENT",
    "LOS", "TOTCHG", "DIED"
  )
  missing_cols <- setdiff(required_cols, colnames(.data))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  col_index <- rlang::sym(paste0("IndexEvent", suffix))
  col_read <- rlang::sym(paste0("Readmit", suffix))
  col_gap <- rlang::sym(paste0("DaysToReadmit", suffix))
  col_total <- rlang::sym(paste0("TotalReadmissions", suffix))
  col_chg <- rlang::sym(paste0("ReadmitCHG", suffix))
  col_died <- rlang::sym(paste0("ReadmitDIED", suffix))
  col_r_row <- rlang::sym(paste0("ReadmissionRow", suffix))

  tbl_base <- .data %>%
    mutate(
      .cr_pseudo_ddate = NRD_DAYSTOEVENT + LOS
    )

  tbl_index <- tbl_base %>%
    filter(
      !is.na(NRD_VISITLINK),
      !is.na(LOS),
      !is.na(NRD_DAYSTOEVENT),
      DMONTH <= !!last_allowable_month,
      DIED == 0,
      !!quo_index
    ) %>%
    select(YEAR, NRD_VISITLINK, KEY_NRD, .cr_pseudo_ddate)

  tbl_candidate <- tbl_base %>%
    filter(
      !is.na(NRD_VISITLINK),
      !is.na(LOS),
      !is.na(NRD_DAYSTOEVENT),
      !!quo_readmit
    ) %>%
    select(YEAR, NRD_VISITLINK, KEY_NRD, NRD_DAYSTOEVENT, TOTCHG, DIED)

  tbl_pairs <- tbl_index %>%
    inner_join(
      tbl_candidate,
      by = c("YEAR", "NRD_VISITLINK"),
      suffix = c("_idx", "_cand")
    ) %>%
    filter(KEY_NRD_idx != KEY_NRD_cand) %>%
    mutate(.cr_gap_days = NRD_DAYSTOEVENT - .cr_pseudo_ddate) %>%
    filter(between(.cr_gap_days, 1L, !!window))

  tbl_first <- tbl_pairs %>%
    group_by(YEAR, KEY_NRD_idx) %>%
    slice_min(order_by = .cr_gap_days, n = 1, with_ties = TRUE) %>%
    ungroup()

  tbl_first_idx <- tbl_first %>%
    group_by(YEAR, KEY_NRD_idx) %>%
    summarise(
      .cr_readmit = 1L,
      .cr_gap = min(.cr_gap_days),
      .cr_charges = if_else(
        sum(if_else(!is.na(TOTCHG), 1L, 0L), na.rm = TRUE) > 0L,
        as.numeric(sum(TOTCHG, na.rm = TRUE)),
        NA_real_
      ),
      .cr_died_r = if_else(
        sum(if_else(!is.na(DIED), 1L, 0L), na.rm = TRUE) > 0L,
        as.integer(max(DIED, na.rm = TRUE)),
        NA_integer_
      ),
      .groups = "drop"
    ) %>%
    rename(KEY_NRD = KEY_NRD_idx)

  tbl_first_rrow <- tbl_first %>%
    transmute(
      YEAR,
      KEY_NRD = KEY_NRD_cand,
      .cr_readmission_row = 1L
    ) %>%
    distinct(YEAR, KEY_NRD, .cr_readmission_row)

  tbl_totals <- tbl_pairs %>%
    group_by(YEAR, KEY_NRD_idx) %>%
    summarise(.cr_total = n_distinct(.cr_gap_days), .groups = "drop") %>%
    rename(KEY_NRD = KEY_NRD_idx)

  tbl_index_flag <- tbl_index %>%
    transmute(YEAR, KEY_NRD, .cr_index_event = 1L)

  tbl_base %>%
    left_join(tbl_index_flag, by = c("YEAR", "KEY_NRD")) %>%
    left_join(tbl_first_idx, by = c("YEAR", "KEY_NRD")) %>%
    left_join(tbl_totals, by = c("YEAR", "KEY_NRD")) %>%
    left_join(tbl_first_rrow, by = c("YEAR", "KEY_NRD")) %>%
    mutate(
      !!col_index := coalesce(.cr_index_event, 0L),
      !!col_read := coalesce(.cr_readmit, 0L),
      !!col_gap := .cr_gap,
      !!col_total := coalesce(as.numeric(.cr_total), 0),
      !!col_chg := .cr_charges,
      !!col_died := .cr_died_r,
      !!col_r_row := coalesce(.cr_readmission_row, 0L)
    ) %>%
    select(
      -.cr_index_event,
      -.cr_readmit,
      -.cr_gap,
      -.cr_total,
      -.cr_charges,
      -.cr_died_r,
      -.cr_readmission_row,
      -.cr_pseudo_ddate
    )
}
