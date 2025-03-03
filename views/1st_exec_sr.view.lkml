view: 1st_exec_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              ROUND(
    COUNT(DISTINCT CASE
        WHEN status = 'SUCCESS'
        THEN CONCAT(
          umn,
          REPLACE(
            JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
            '"', ''
          )
        )
        ELSE NULL
      END
    ) * 100.0 /
    COUNT(DISTINCT CONCAT(
        umn,
        REPLACE(
          JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
          '"', ''
        )
      )
    ),
    2
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
      )SELECT
          created_date,
          -- Convert to VARCHAR with '%' in final select
          CONCAT(CAST(MAX(CASE WHEN handle = 'ptaxis' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptaxis SR",
          CONCAT(CAST(MAX(CASE WHEN handle = 'pthdfc' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "pthdfc SR",
          CONCAT(CAST(MAX(CASE WHEN handle = 'ptsbi' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptsbi SR",
          CONCAT(CAST(MAX(CASE WHEN handle = 'ptyes' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptyes SR",
          -- Calculate Average SR and then format as percentage
          CONCAT(CAST(ROUND(AVG(sr), 2) AS VARCHAR), '%') AS "Average SR"
      FROM handle_data
      GROUP BY created_date
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
