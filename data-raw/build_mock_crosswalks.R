# Mock clinical crosswalks for internal testing

load("R/sysdata.rda")

nrd_all_dx_codes <- c(
  "E119", "I10", "Z794", "S82001A", "N179", "J449", "I214", "R509",
  "K219", "M545", "A419", "G4733", "F329", "C50911", "D649", "Z001"
)

nrd_all_pr_codes <- c(
  "0D164ZA", "0JH60DZ", "30233N1", "02703ZZ", "5A1955Z", "0BH17EZ",
  "0W3P8ZZ", "3E03329", "0DJD8ZZ", "0UTC7ZZ", "3E0P3GC", "0WQF0ZZ"
)

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
  nrd_all_dx_codes,
  nrd_all_pr_codes,
  nrd_elixhauser_xwalk,
  nrd_ccsr_xwalk,
  internal = TRUE,
  overwrite = TRUE
)
