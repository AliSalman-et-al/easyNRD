# Build internal NRD lookup tables as relational tibbles.

lookup_sex <- tibble::tibble(
  code = c(0L, 1L),
  label = c("Male", "Female")
)

lookup_income_quartile <- tibble::tibble(
  code = c(1L, 2L, 3L, 4L),
  label = c(
    "0-25th percentile",
    "26th to 50th percentile",
    "51st to 75th percentile",
    "76th to 100th percentile"
  )
)

lookup_payer <- tibble::tibble(
  code = c(1L, 2L, 3L, 4L, 5L, 6L),
  label = c("Medicare", "Medicaid", "Private", "Self-pay", "No charge", "Other")
)

lookup_urbanicity <- tibble::tibble(
  code = c(1L, 2L, 3L, 4L, 5L, 6L),
  label = c(
    "Central metro >=1 million",
    "Fringe metro >=1 million",
    "Metro 250,000-999,999",
    "Metro 50,000-249,999",
    "Micropolitan",
    "Other"
  )
)

lookup_h_contrl <- tibble::tibble(
  code = c(1L, 2L, 3L),
  label = c("Government, nonfederal", "Private, not-profit", "Private, invest-own")
)

lookup_hosp_bedsize <- tibble::tibble(
  code = c(1L, 2L, 3L),
  label = c("Small", "Medium", "Large")
)

lookup_hosp_ur_teach <- tibble::tibble(
  code = c(0L, 1L, 2L),
  label = c("Metropolitan, non-teaching", "Metropolitan, teaching", "Non-metropolitan")
)

lookup_hosp_urcat4 <- tibble::tibble(
  code = c(1L, 2L, 3L, 4L),
  label = c(
    "Large metropolitan >=1 million",
    "Small metropolitan <1 million",
    "Micropolitan",
    "Non-urban"
  )
)

lookup_dispuniform <- tibble::tibble(
  code = c(1L, 2L, 5L, 6L, 7L, 20L, 99L),
  label = c(
    "Routine discharge to home/self-care",
    "Transfer to another short-term hospital",
    "Transfer to SNF/intermediate/other facility",
    "Home health care",
    "Left against medical advice",
    "Died in hospital",
    "Alive, destination unknown"
  )
)

lookup_samedayevent <- tibble::tibble(
  code = c(0L, 1L, 2L, 3L, 4L),
  label = c(
    "No same-day event",
    "Same-day event: 2 hospitals (transfer)",
    "Same-day event: 2 hospitals (separate stays)",
    "Same-day event: 2 discharges, same hospital",
    "Same-day event: 3+ discharges"
  )
)

nrd_lookup_sex <- lookup_sex
nrd_lookup_income_quartile <- lookup_income_quartile
nrd_lookup_payer <- lookup_payer
nrd_lookup_urbanicity <- lookup_urbanicity
nrd_lookup_h_contrl <- lookup_h_contrl
nrd_lookup_hosp_bedsize <- lookup_hosp_bedsize
nrd_lookup_hosp_ur_teach <- lookup_hosp_ur_teach
nrd_lookup_hosp_urcat4 <- lookup_hosp_urcat4
nrd_lookup_dispuniform <- lookup_dispuniform
nrd_lookup_samedayevent <- lookup_samedayevent

usethis::use_data(
  nrd_lookup_sex,
  nrd_lookup_income_quartile,
  nrd_lookup_payer,
  nrd_lookup_urbanicity,
  nrd_lookup_h_contrl,
  nrd_lookup_hosp_bedsize,
  nrd_lookup_hosp_ur_teach,
  nrd_lookup_hosp_urcat4,
  nrd_lookup_dispuniform,
  nrd_lookup_samedayevent,
  internal = TRUE,
  overwrite = TRUE
)
