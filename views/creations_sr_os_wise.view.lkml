view: creations_sr_os_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') AS Os_App,
            ROUND(
    COUNT(DISTINCT CASE
        WHEN status = 'SUCCESS'
        THEN umn
        ELSE NULL
      END
    ) * 100.0 /
   NULLIF(COUNT(DISTINCT umn), 0), 2
  ) AS sr_value
  FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
          GROUP BY
              1, 2
      )
      SELECT
          created_date,
          MAX(CASE WHEN Os_App = 'android' THEN CONCAT(CAST(ROUND(sr_value, 2) AS VARCHAR), '%') ELSE NULL END) AS "Android SR",
          MAX(CASE WHEN Os_App LIKE 'iOS%' THEN CONCAT(CAST(ROUND(sr_value, 2) AS VARCHAR), '%') ELSE NULL END) AS "iOS SR",
          CONCAT(CAST(ROUND(AVG(sr_value), 2) AS VARCHAR), '%') AS "Avg SR"
      FROM
          handle_data
      GROUP BY
          created_date
      ORDER BY
          created_date DESC
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

  dimension: android_sr {
    type: string
    label: "Android SR"
    sql: ${TABLE}."Android SR" ;;
  }

  dimension: i_os_sr {
    type: string
    label: "iOS SR"
    sql: ${TABLE}."iOS SR" ;;
  }

  dimension: avg_sr {
    type: string
    label: "Avg SR"
    sql: ${TABLE}."Avg SR" ;;
  }

  set: detail {
    fields: [created_date, android_sr, i_os_sr, avg_sr]
  }
}
