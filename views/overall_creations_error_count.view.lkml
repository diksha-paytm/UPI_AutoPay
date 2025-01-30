view: overall_creations_error_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.npci_resp_code,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated IS NOT NULL
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status = 'FAILURE'
              AND SUBSTRING(ti.umn, POSITION('@' IN ti.umn) + 1) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm','paytm')
              AND ti.npci_resp_code IN ('UM3', 'ZA', 'UM8-ZM', 'UM8', 'UM1', 'UM9', 'MD00', 'U66', 'UM8-Z6', 'UM2')
          GROUP BY
              DATE(ti.created_on),
              ti.npci_resp_code
      ),
      pivoted_data AS (
          SELECT
              created_date,
              npci_resp_code,
              failure
          FROM
              handle_data
      )
      SELECT
          created_date,
          MAX(CASE WHEN npci_resp_code = 'UM3' THEN failure ELSE NULL END) AS "UM3 Failure",
          MAX(CASE WHEN npci_resp_code = 'ZA' THEN failure ELSE NULL END) AS "ZA Failure",
          MAX(CASE WHEN npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "UM8-ZM Failure",
          MAX(CASE WHEN npci_resp_code = 'UM8' THEN failure ELSE NULL END) AS "UM8 Failure",
          MAX(CASE WHEN npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "UM1 Failure",
          MAX(CASE WHEN npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "UM9 Failure",
          MAX(CASE WHEN npci_resp_code = 'MD00' THEN failure ELSE NULL END) AS "MD00 Failure",
          MAX(CASE WHEN npci_resp_code = 'U66' THEN failure ELSE NULL END) AS "U66 Failure",
          MAX(CASE WHEN npci_resp_code = 'UM8-Z6' THEN failure ELSE NULL END) AS "UM8-Z6 Failure",
          MAX(CASE WHEN npci_resp_code = 'UM2' THEN failure ELSE NULL END) AS "UM2 Failure"
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

  dimension: um3_failure {
    type: number
    label: "UM3 Failure"
    sql: ${TABLE}."UM3 Failure" ;;
  }

  dimension: za_failure {
    type: number
    label: "ZA Failure"
    sql: ${TABLE}."ZA Failure" ;;
  }

  dimension: um8zm_failure {
    type: number
    label: "UM8-ZM Failure"
    sql: ${TABLE}."UM8-ZM Failure" ;;
  }

  dimension: um8_failure {
    type: number
    label: "UM8 Failure"
    sql: ${TABLE}."UM8 Failure" ;;
  }

  dimension: um1_failure {
    type: number
    label: "UM1 Failure"
    sql: ${TABLE}."UM1 Failure" ;;
  }

  dimension: um9_failure {
    type: number
    label: "UM9 Failure"
    sql: ${TABLE}."UM9 Failure" ;;
  }

  dimension: md00_failure {
    type: number
    label: "MD00 Failure"
    sql: ${TABLE}."MD00 Failure" ;;
  }

  dimension: u66_failure {
    type: number
    label: "U66 Failure"
    sql: ${TABLE}."U66 Failure" ;;
  }

  dimension: um8z6_failure {
    type: number
    label: "UM8-Z6 Failure"
    sql: ${TABLE}."UM8-Z6 Failure" ;;
  }

  dimension: um2_failure {
    type: number
    label: "UM2 Failure"
    sql: ${TABLE}."UM2 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      um3_failure,
      za_failure,
      um8zm_failure,
      um8_failure,
      um1_failure,
      um9_failure,
      md00_failure,
      u66_failure,
      um8z6_failure,
      um2_failure
    ]
  }
}
