# pattern mode SQL snapshot is stable

    Code
      cat(sql)
    Output
      SELECT
        q01.*,
        COALESCE((REGEXP_MATCHES(I10_DX1, '^I21') OR REGEXP_MATCHES(I10_DX2, '^I21')), FALSE) AS is_ami
      FROM (
        SELECT
          "YEAR",
          NRD_VISITLINK,
          KEY_NRD,
          HOSP_NRD,
          DISCWT,
          NRD_STRATUM,
          NRD_DaysToEvent AS NRD_DAYSTOEVENT,
          LOS,
          DMONTH,
          DIED,
          FEMALE,
          I10_DX1,
          I10_DX2,
          I10_PR1,
          I10_PR2
        FROM arrow_tbl
      ) q01

# codes mode SQL snapshot is stable

    Code
      cat(sql)
    Output
      SELECT
        q01.*,
        COALESCE((I10_DX1 IN ('I214', 'I219') OR I10_DX2 IN ('I214', 'I219')), FALSE) AS is_ami
      FROM (
        SELECT
          "YEAR",
          NRD_VISITLINK,
          KEY_NRD,
          HOSP_NRD,
          DISCWT,
          NRD_STRATUM,
          NRD_DaysToEvent AS NRD_DAYSTOEVENT,
          LOS,
          DMONTH,
          DIED,
          FEMALE,
          I10_DX1,
          I10_DX2,
          I10_PR1,
          I10_PR2
        FROM arrow_tbl
      ) q01

