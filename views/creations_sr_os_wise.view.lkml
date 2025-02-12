view: creations_sr_os_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') AS Os_App,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1, 2
      ),
      pivoted_data AS (
          SELECT
              created_date,
              Os_App,
              success,
              failure,
              (success * 100.0 / NULLIF(success + failure, 0)) AS sr_value -- Numeric success rate for averaging
          FROM
              handle_data
      )
      SELECT
          created_date,
          MAX(CASE WHEN Os_App = 'android' THEN CONCAT(CAST(ROUND(sr_value, 2) AS VARCHAR), '%') ELSE NULL END) AS "Android SR",
          MAX(CASE WHEN Os_App LIKE 'iOS%' THEN CONCAT(CAST(ROUND(sr_value, 2) AS VARCHAR), '%') ELSE NULL END) AS "iOS SR",
          CONCAT(CAST(ROUND(AVG(sr_value), 2) AS VARCHAR), '%') AS "Avg SR"
      FROM
          pivoted_data
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
