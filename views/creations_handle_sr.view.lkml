view: creations_handle_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              ROUND(
    COUNT(DISTINCT CASE
        WHEN status = 'SUCCESS'
        THEN umn
        ELSE NULL
      END
    ) * 100.0 /
    NULLIF(COUNT(DISTINCT umn), 0), 2
  ) AS sr
  FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
          GROUP BY DATE(ti.created_on), SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'ptaxis' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptaxis SR",
          MAX(CASE WHEN handle = 'pthdfc' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "pthdfc SR",
          MAX(CASE WHEN handle = 'ptsbi' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptsbi SR",
          MAX(CASE WHEN handle = 'ptyes' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptyes SR",

      -- Average SR calculation
      CONCAT(
      CAST(ROUND(
      SUM(sr) / NULLIF(COUNT(sr), 0),
      2
      ) AS VARCHAR), '%'
      ) AS "Average SR"

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
