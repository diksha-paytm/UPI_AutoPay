view: creations_failure_mode_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(
                  JSON_QUERY(ti.extended_info, 'strict $.initiationMode'),
                  '"',
                  ''
              ) AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE')
              AND SUBSTRING(ti.umn, POSITION('@' IN ti.umn) + 1) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm', 'paytm')
          GROUP BY
              1, 2
      ),
      pivoted_data AS (
          SELECT
              created_date,
              initiation_mode,
              failure
          FROM
              handle_data
          WHERE
              initiation_mode IN ('00', '04', '13')
      )
      SELECT
          created_date,
          MAX(CASE WHEN initiation_mode = '00' THEN failure ELSE NULL END) AS "Collect Failure",
          MAX(CASE WHEN initiation_mode = '04' THEN failure ELSE NULL END) AS "Intent Failure",
          MAX(CASE WHEN initiation_mode = '13' THEN failure ELSE NULL END) AS "QR Failure"
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

  dimension: collect_failure {
    type: number
    label: "Collect Failure"
    sql: ${TABLE}."Collect Failure" ;;
  }

  dimension: intent_failure {
    type: number
    label: "Intent Failure"
    sql: ${TABLE}."Intent Failure" ;;
  }

  dimension: qr_failure {
    type: number
    label: "QR Failure"
    sql: ${TABLE}."QR Failure" ;;
  }

  set: detail {
    fields: [created_date, collect_failure, intent_failure, qr_failure]
  }
}
