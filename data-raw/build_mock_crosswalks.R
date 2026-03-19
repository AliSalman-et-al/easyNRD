# Mock clinical crosswalks for internal testing

load("R/sysdata.rda")

nrd_elixhauser_xwalk <- tibble::tibble(
  ICD10_Code = c("E119", "I10", "N179", "J449"),
  Uncomplicated_Diabetes = c(1L, 0L, 0L, 0L),
  Hypertension = c(0L, 1L, 0L, 0L),
  Renal_Failure = c(0L, 0L, 1L, 0L),
  Chronic_Pulmonary_Disease = c(0L, 0L, 0L, 1L)
)

nrd_ccsr_xwalk <- tibble::tibble(
  ICD10_Code = c("E119", "I10", "N179", "J449"),
  END002 = c(1L, 0L, 0L, 0L),
  CIR007 = c(0L, 1L, 0L, 0L),
  GEN003 = c(0L, 0L, 1L, 0L),
  RSP008 = c(0L, 0L, 0L, 1L)
)

usethis::use_data(
  nrd_dict,
  nrd_elixhauser_xwalk,
  nrd_ccsr_xwalk,
  internal = TRUE,
  overwrite = TRUE
)
