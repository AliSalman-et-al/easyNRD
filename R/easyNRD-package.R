#' easyNRD: Tools for HCUP-NRD Workflows
#'
#' Provides lazy, SQL-translatable tools for NRD label augmentation, episode
#' consolidation, and readmission linkage.
#'
#' @keywords internal
#' @importFrom rlang :=
#' @importFrom utils globalVariables
"_PACKAGE"

utils::globalVariables(
  c(
    ".nrd_admit_day", ".nrd_approx_discharge_doy", ".nrd_contiguous",
    ".nrd_discharge_day_row", ".nrd_episode_rows", ".nrd_gap_days", ".nrd_is_leap",
    ".nrd_label", ".nrd_los", ".nrd_month_start", ".nrd_prev_discharge",
    ".nrd_prev_trigger", ".nrd_rank", ".nrd_row_number", ".nrd_transfer_trigger",
    ".nrd_year_days", "DIED", "DISPUNIFORM", "DMONTH", "Days_to_End_of_Year",
    "Episode_Admission_Day", "Episode_Admission_Day_cand", "Episode_DMONTH",
    "Episode_Discharge_Day", "Episode_Discharge_Day_idx", "Episode_ID",
    "Episode_ID_cand", "Episode_ID_idx", "Episode_Index_KEY_NRD", "Episode_KEY_NRD",
    "Episode_KEY_NRD_cand", "Episode_KEY_NRD_idx", "Episode_LOS", "IndexEvent",
    "Is_Readmission_Event", "KEY_NRD", "LOS", "Linked_To_Target_Index",
    "NRD_DAYSTOEVENT", "NRD_VISITLINK", "SAMEDAYEVENT", "TOTCHG", "YEAR", "first_readmit_gap", "label",
    "outcome_status", "time_to_event"
  )
)
