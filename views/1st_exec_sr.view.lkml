view: 1st_exec_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              ROUND(
                  COUNT(DISTINCT CASE
                      WHEN status = 'SUCCESS' THEN CONCAT(
                          umn,
                          REPLACE(JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
                      )
                  END) * 100.0 /
                  COUNT(DISTINCT CONCAT(
                      umn,
                      REPLACE(JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
                  )), 2
              ) AS sr
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
          GROUP BY 1, 2
      ),
      aggregated_data AS (
          SELECT
              created_date,
              MAX(CASE WHEN handle = 'ptaxis' THEN sr END) AS ptaxis_sr,
              MAX(CASE WHEN handle = 'pthdfc' THEN sr END) AS pthdfc_sr,
              MAX(CASE WHEN handle = 'ptsbi' THEN sr END) AS ptsbi_sr,
              MAX(CASE WHEN handle = 'ptyes' THEN sr END) AS ptyes_sr
          FROM handle_data
          GROUP BY created_date
      )
      SELECT
          created_date,
          CONCAT(CAST(ptaxis_sr AS VARCHAR), '%') AS "ptaxis SR",
          CONCAT(CAST(pthdfc_sr AS VARCHAR), '%') AS "pthdfc SR",
          CONCAT(CAST(ptsbi_sr AS VARCHAR), '%') AS "ptsbi SR",
          CONCAT(CAST(ptyes_sr AS VARCHAR), '%') AS "ptyes SR",
          -- Fix: Correctly handle NULLs and divide by the number of non-null values
          CONCAT(
              CAST(
                  ROUND(
                      (COALESCE(ptaxis_sr, 0) +
                       COALESCE(pthdfc_sr, 0) +
                       COALESCE(ptsbi_sr, 0) +
                       COALESCE(ptyes_sr, 0)) /
                      NULLIF(
                          (CASE WHEN ptaxis_sr IS NOT NULL THEN 1 ELSE 0 END +
                           CASE WHEN pthdfc_sr IS NOT NULL THEN 1 ELSE 0 END +
                           CASE WHEN ptsbi_sr IS NOT NULL THEN 1 ELSE 0 END +
                           CASE WHEN ptyes_sr IS NOT NULL THEN 1 ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS "Average SR"
      FROM aggregated_data
      ORDER BY created_date DESC
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: created_date {
    type: date
    sql: ${TABLE}.created_date ;;
  }

  dimension: ptaxis_sr {
    type: string
    label: "ptaxis SR"
    sql: ${TABLE}."ptaxis SR" ;;
  }

  dimension: pthdfc_sr {
    type: string
    label: "pthdfc SR"
    sql: ${TABLE}."pthdfc SR" ;;
  }

  dimension: ptsbi_sr {
    type: string
    label: "ptsbi SR"
    sql: ${TABLE}."ptsbi SR" ;;
  }

  dimension: ptyes_sr {
    type: string
    label: "ptyes SR"
    sql: ${TABLE}."ptyes SR" ;;
  }

  dimension: average_sr {
    type: string
    label: "Average SR"
    sql: ${TABLE}."Average SR" ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_sr,
      pthdfc_sr,
      ptsbi_sr,
      ptyes_sr,
      average_sr
    ]
  }
}
