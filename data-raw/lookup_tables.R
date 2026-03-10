nrd_dict <- list(
  FEMALE = c("0" = "Male", "1" = "Female"),
  PAY1 = c(
    "1" = "Medicare",
    "2" = "Medicaid",
    "3" = "Private",
    "4" = "Self-pay",
    "5" = "No charge",
    "6" = "Other"
  ),
  ZIPINC_QRTL = c(
    "1" = "0-25th percentile",
    "2" = "26th to 50th percentile",
    "3" = "51st to 75th percentile",
    "4" = "76th to 100th percentile"
  ),
  PL_NCHS = c(
    "1" = "Central metro >=1 million",
    "2" = "Fringe metro >=1 million",
    "3" = "Metro 250,000-999,999",
    "4" = "Metro 50,000-249,999",
    "5" = "Micropolitan",
    "6" = "Other"
  ),
  H_CONTRL = c(
    "1" = "Government, nonfederal",
    "2" = "Private, not-profit",
    "3" = "Private, invest-own"
  ),
  HOSP_BEDSIZE = c("1" = "Small", "2" = "Medium", "3" = "Large"),
  HOSP_UR_TEACH = c(
    "0" = "Metropolitan, non-teaching",
    "1" = "Metropolitan, teaching",
    "2" = "Non-metropolitan"
  ),
  HOSP_URCAT4 = c(
    "1" = "Large metropolitan >=1 million",
    "2" = "Small metropolitan <1 million",
    "3" = "Micropolitan",
    "4" = "Non-urban"
  ),
  DISPUNIFORM = c(
    "1" = "Routine discharge to home/self-care",
    "2" = "Transfer to another short-term hospital",
    "5" = "Transfer to SNF/intermediate/other facility",
    "6" = "Home health care",
    "7" = "Left against medical advice",
    "20" = "Died in hospital",
    "99" = "Alive, destination unknown"
  ),
  SAMEDAYEVENT = c(
    "0" = "No same-day event",
    "1" = "Same-day event: 2 hospitals (transfer)",
    "2" = "Same-day event: 2 hospitals (separate stays)",
    "3" = "Same-day event: 2 discharges, same hospital",
    "4" = "Same-day event: 3+ discharges"
  )
)

usethis::use_data(nrd_dict, internal = TRUE, overwrite = TRUE)
