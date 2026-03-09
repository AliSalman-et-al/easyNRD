# Load required libraries
library(arrow) # For handling Parquet files
library(dplyr) # For data manipulation
library(stringr) # For string manipulation
library(duckdb) # For DuckDB
library(dbplyr) # For DuckDB dplyr

# Define the function
clean_NRD_base_data <- function(datasets) {
  # Define the vector columns
  icd_dx_columns <- paste0("I10_DX", 1:40)
  icd_pr_columns <- paste0("I10_PR", 1:25)
  icd_pr_day_columns <- paste0("PRDAY", 1:25)
  icd_dx_secondary_columns <- paste0("I10_DX", 2:40)
  icd_pr_secondary_columns <- paste0("I10_PR", 2:25)
  elix_dx_columns <- paste0("ynel", 1:31)
  charl_dx_columns <- paste0("ynch", 1:17)

  # Combine datasets and clean data
  dta_clean <- arrow::open_dataset(datasets, unify_schemas = TRUE, format = "parquet") |>
    to_duckdb() |>
    mutate(
      # Combine ICD DX & PR columns into one
      DX10_Combined = str_c(!!!syms(icd_dx_columns), sep = ", "),
      DX10_Secondary  = str_c(!!!syms(icd_dx_secondary_columns), sep = ", "),
      PR10_Combined = str_c(!!!syms(icd_pr_columns), sep = ", "),
      PR10_Secondary  = str_c(!!!syms(icd_pr_secondary_columns), sep = ", "),
      # Admission / discharge
      AWEEKEND = case_when(
        AWEEKEND == 0 ~ "Monday-Friday",
        AWEEKEND == 1 ~ "Saturday-Sunday"
      ),
      MONTH = case_when(
        DMONTH == 1  ~ "January",
        DMONTH == 2  ~ "February",
        DMONTH == 3  ~ "March",
        DMONTH == 4  ~ "April",
        DMONTH == 5  ~ "May",
        DMONTH == 6  ~ "June",
        DMONTH == 7  ~ "July",
        DMONTH == 8  ~ "August",
        DMONTH == 9  ~ "September",
        DMONTH == 10 ~ "October",
        DMONTH == 11 ~ "November",
        DMONTH == 12 ~ "December"
      ),
      DQTR = case_when(
        DQTR == 1 ~ "Q1",
        DQTR == 2 ~ "Q2",
        DQTR == 3 ~ "Q3",
        DQTR == 4 ~ "Q4"
      ),
      DISPUNIFORM = case_when(
        DISPUNIFORM == 1  ~ "Routine discharge to home/self-care",
        DISPUNIFORM == 2  ~ "Transfer to another short-term hospital",
        DISPUNIFORM == 5  ~ "Transfer to SNF / intermediate / other facility",
        DISPUNIFORM == 6  ~ "Home health care",
        DISPUNIFORM == 7  ~ "Left against medical advice",
        DISPUNIFORM == 20 ~ "Died in hospital",
        DISPUNIFORM == 99 ~ "Alive, destination unknown"
      ),
      ELECTIVE = case_when(
        ELECTIVE == 0 ~ "Non-elective",
        ELECTIVE == 1 ~ "Elective"
      ),
      HCUP_ED = case_when(
        HCUP_ED == 0 ~ "No",
        HCUP_ED %in% c(1, 2, 3, 4) ~ "Yes"
      ),
      DIED = as.numeric(DIED),
      YEAR = as.numeric(YEAR),
      # Patient factors
      AGE = as.numeric(AGE),
      FEMALE = case_when(
        FEMALE == 0 ~ "Male",
        FEMALE == 1 ~ "Female"
      ),
      PAY1 = case_when(
        PAY1 == 1 ~ "Medicare",
        PAY1 == 2 ~ "Medicaid",
        PAY1 == 3 ~ "Private",
        PAY1 == 4 ~ "Self-pay",
        PAY1 == 5 ~ "No charge",
        PAY1 == 6 ~ "Other"
      ),
      PL_NCHS = case_when(
        PL_NCHS == 1 ~ "Central metro ≥1 million",
        PL_NCHS == 2 ~ "Fringe metro ≥1 million",
        PL_NCHS == 3 ~ "Metro 250,000-999,999",
        PL_NCHS == 4 ~ "Metro 50,000-249,999",
        PL_NCHS == 5 ~ "Micropolitan",
        PL_NCHS == 6 ~ "Other"
      ),
      ZIPINC_QRTL = case_when(
        ZIPINC_QRTL == 1 ~ "0-25th percentile",
        ZIPINC_QRTL == 2 ~ "26th to 50th percentile",
        ZIPINC_QRTL == 3 ~ "51st to 75th percentile",
        ZIPINC_QRTL == 4 ~ "76th to 100th percentile"
      ),

      # Readmission-specific
      REHABTRANSFER = case_when(
        REHABTRANSFER == 0 ~ "No",
        REHABTRANSFER == 1 ~ "Yes"
      ),
      RESIDENT = case_when(
        RESIDENT == 0 ~ "Non-resident",
        RESIDENT == 1 ~ "Resident"
      ),
      SAMEDAYEVENT = case_when(
        SAMEDAYEVENT == 0 ~ "No same-day event",
        SAMEDAYEVENT == 1 ~ "Same-day event: 2 hospitals (transfer)",
        SAMEDAYEVENT == 2 ~ "Same-day event: 2 hospitals (separate stays)",
        SAMEDAYEVENT == 3 ~ "Same-day event: 2 discharges, same hospital",
        SAMEDAYEVENT == 4 ~ "Same-day event: 3+ discharges"
      ),
      NRD_DAYSTOEVENT = as.numeric(NRD_DAYSTOEVENT),

      # Resource use
      LOS    = as.numeric(LOS),
      TOTCHG = as.numeric(TOTCHG),

      # Weights & strata
      DISCWT      = as.numeric(DISCWT),
      NRD_STRATUM = as.numeric(NRD_STRATUM),
      HOSP_NRD    = as.numeric(HOSP_NRD),

      # Hospital factors
      H_CONTRL = case_when(
        H_CONTRL == 1 ~ "Government, nonfederal",
        H_CONTRL == 2 ~ "Private, not-profit",
        H_CONTRL == 3 ~ "Private, invest-own"
      ),
      HOSP_BEDSIZE = case_when(
        HOSP_BEDSIZE == 1 ~ "Small",
        HOSP_BEDSIZE == 2 ~ "Medium",
        HOSP_BEDSIZE == 3 ~ "Large"
      ),
      HOSP_UR_TEACH = case_when(
        HOSP_UR_TEACH == 0 ~ "Metropolitan, non-teaching",
        HOSP_UR_TEACH == 1 ~ "Metropolitan, teaching",
        HOSP_UR_TEACH == 2 ~ "Non-metropolitan"
      ),
      HOSP_URCAT4 = case_when(
        HOSP_URCAT4 == 1 ~ "Large metropolitan ≥1 million",
        HOSP_URCAT4 == 2 ~ "Small metropolitan <1 million",
        HOSP_URCAT4 == 3 ~ "Micropolitan",
        HOSP_URCAT4 == 4 ~ "Non-urban"
      ),

      # # APR-DRG severity
      # APRDRG_Risk_Mortality = case_when(
      #   APRDRG_Risk_Mortality == 0 ~ "None specified",
      #   APRDRG_Risk_Mortality == 1 ~ "Minor",
      #   APRDRG_Risk_Mortality == 2 ~ "Moderate",
      #   APRDRG_Risk_Mortality == 3 ~ "Major",
      #   APRDRG_Risk_Mortality == 4 ~ "Extreme"
      # ),
      # APRDRG_Severity = case_when(
      #   APRDRG_Severity == 0 ~ "None specified",
      #   APRDRG_Severity == 1 ~ "Minor",
      #   APRDRG_Severity == 2 ~ "Moderate",
      #   APRDRG_Severity == 3 ~ "Major",
      #   APRDRG_Severity == 4 ~ "Extreme"
      # ),

      # Elixhausen comorbidity index
      across(c(elix_dx_columns), ~ case_when(. == 0 ~ "No", . == 1 ~ "Yes")),
      elixsum = as.numeric(elixsum),
      # Charlson comorbidity index
      across(c(charl_dx_columns), ~ case_when(. == 0 ~ "No", . == 1 ~ "Yes")),
      charlindex = as.numeric(charlindex),
      grpci = case_when(
        grpci == 0 ~ "No comorbidities",
        grpci == 1 ~ "One comorbidity",
        grpci == 2 ~ "Two or more comorbidities"
      ),

      isTRAUMA = case_when(
        str_detect(DX10_Combined, "S.*|T.*|U.*|V.*|W.*|X.*|Y.*") ~ "Yes",
        .default = "No"
      ),

      # Diagnosis and procedure information
      I10_NDX = as.numeric(I10_NDX),
      I10_NPR = as.numeric(I10_NPR),
      across(c(icd_pr_day_columns), as.numeric)
    )

  return(dta_clean)
}

