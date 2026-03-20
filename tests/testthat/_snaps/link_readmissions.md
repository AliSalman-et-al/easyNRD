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
        SELECT checkpoint_tbl.*, gap, readmit_KEY_NRD, readmit_severity
        FROM checkpoint_tbl
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
              "YEAR",
              NRD_VISITLINK,
              KEY_NRD_idx,
              DTE_idx,
              LOS_idx,
              KEY_NRD_cand,
              DTE_cand,
              KEY_NRD,
              severity,
              gap,
              ROW_NUMBER() OVER (PARTITION BY "YEAR", NRD_VISITLINK, KEY_NRD_idx ORDER BY KEY_NRD_cand) AS col03
            FROM (
              SELECT
                "YEAR",
                NRD_VISITLINK,
                KEY_NRD_idx,
                DTE_idx,
                LOS_idx,
                KEY_NRD_cand,
                DTE_cand,
                KEY_NRD,
                severity,
                gap,
                RANK() OVER (PARTITION BY "YEAR", NRD_VISITLINK, KEY_NRD_idx ORDER BY DTE_cand) AS col02
              FROM (
                SELECT
                  q01.*,
                  RANK() OVER (PARTITION BY "YEAR", NRD_VISITLINK, KEY_NRD_idx ORDER BY gap) AS col01
                FROM (
                  SELECT q01.*, (DTE_cand - DTE_idx) - LOS_idx AS gap
                  FROM (
                    SELECT LHS.*, KEY_NRD_cand, DTE_cand, KEY_NRD, severity
                    FROM (
                      SELECT
                        "YEAR",
                        NRD_VISITLINK,
                        KEY_NRD AS KEY_NRD_idx,
                        NRD_DAYSTOEVENT AS DTE_idx,
                        LOS AS LOS_idx
                      FROM checkpoint_tbl
                      WHERE (IndexEvent = 1)
                    ) LHS
                    INNER JOIN (
                      SELECT
                        "YEAR",
                        NRD_VISITLINK,
                        KEY_NRD AS KEY_NRD_cand,
                        NRD_DAYSTOEVENT AS DTE_cand,
                        KEY_NRD,
                        severity
                      FROM checkpoint_tbl
                      WHERE (is_readmit = 1)
                    ) RHS
                      ON (
                        LHS."YEAR" = RHS."YEAR" AND
                        LHS.NRD_VISITLINK = RHS.NRD_VISITLINK
                      )
                  ) q01
                  WHERE (KEY_NRD_cand != KEY_NRD_idx)
                ) q01
                WHERE (gap >= 1) AND (gap <= 30)
              ) q01
              WHERE (col01 <= 1)
            ) q01
            WHERE (col02 <= 1)
          ) q01
          WHERE (col03 <= 1)
        ) RHS
          ON (
            checkpoint_tbl."YEAR" = RHS."YEAR" AND
            checkpoint_tbl.NRD_VISITLINK = RHS.NRD_VISITLINK AND
            checkpoint_tbl.KEY_NRD = RHS.KEY_NRD_idx
          )
      ) q01

