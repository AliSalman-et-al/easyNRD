# final SQL snapshot is stable

    Code
      cat(sql)
    Output
      SELECT
        "YEAR",
        NRD_VISITLINK,
        KEY_NRD,
        NRD_DAYSTOEVENT,
        LOS,
        DMONTH,
        DIED,
        is_index,
        is_readmit,
        severity,
        IndexEvent,
        readmit_KEY_NRD,
        readmit_severity,
        CASE
      WHEN ".nrd_died_at_index" THEN 0.0
      WHEN (IndexEvent = 1 AND NOT((gap IS NULL))) THEN (CAST(gap AS DOUBLE))
      WHEN (IndexEvent = 1) THEN (CAST(CASE WHEN ((365 - ".nrd_censor_day") < 0) THEN 0 WHEN NOT ((365 - ".nrd_censor_day") < 0) THEN (CASE WHEN ((365 - ".nrd_censor_day") < 30) THEN (365 - ".nrd_censor_day") WHEN NOT ((365 - ".nrd_censor_day") < 30) THEN 30 END) END AS DOUBLE))
      ELSE NULL
      END AS time_to_event,
        CASE
      WHEN ".nrd_died_at_index" THEN 'Died at Index'
      WHEN (IndexEvent = 1 AND NOT((gap IS NULL))) THEN 'Readmitted'
      WHEN (IndexEvent = 1) THEN 'Censored'
      ELSE NULL
      END AS outcome_status
      FROM (
        SELECT LHS.*, gap, readmit_KEY_NRD, readmit_severity
        FROM (
          SELECT
            q01.*,
            COALESCE(is_index = 1, FALSE) AND is_index_eligible AND DIED = 1 AS ".nrd_died_at_index",
            CASE WHEN (is_index_eligible AND DIED = 0 AND COALESCE(is_index = 1, FALSE)) THEN 1 ELSE 0 END AS IndexEvent,
            NRD_DAYSTOEVENT + LOS AS ".nrd_censor_day",
            COALESCE(is_readmit = 1, FALSE) AS ".nrd_readmit_eligible"
          FROM (
            SELECT q01.*, DMONTH <= 11.0 AS is_index_eligible
            FROM (
              SELECT
                "YEAR",
                NRD_VISITLINK,
                KEY_NRD,
                NRD_DaysToEvent AS NRD_DAYSTOEVENT,
                LOS,
                DMONTH,
                DIED,
                is_index,
                is_readmit,
                severity
              FROM arrow_tbl
            ) q01
          ) q01
        ) LHS
        LEFT JOIN (
          SELECT
            "YEAR",
            NRD_VISITLINK,
            KEY_NRD_idx,
            gap,
            KEY_NRD AS readmit_KEY_NRD,
            severity AS readmit_severity
          FROM (
            SELECT
              checkpoint_tbl.*,
              ROW_NUMBER() OVER (PARTITION BY "YEAR", NRD_VISITLINK, KEY_NRD_idx ORDER BY KEY_NRD_cand) AS col01
            FROM checkpoint_tbl
          ) q01
          WHERE (col01 <= 1)
        ) RHS
          ON (
            LHS."YEAR" = RHS."YEAR" AND
            LHS.NRD_VISITLINK = RHS.NRD_VISITLINK AND
            LHS.KEY_NRD = RHS.KEY_NRD_idx
          )
      ) q01

