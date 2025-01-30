view: overall_1st_exec_error_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.npci_resp_code,
              count(
          distinct concat(
            umn,
            replace(
              json_query(
                extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            )
          )
        ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND ti.status = 'FAILURE'
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
          MAX(CASE WHEN npci_resp_code = 'NU' THEN failure ELSE NULL END) AS "NU Failure",
          MAX(CASE WHEN npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "U30-Z9 Failure",
          MAX(CASE WHEN npci_resp_code = 'U30-YC' THEN failure ELSE NULL END) AS "U30-YC Failure",
          MAX(CASE WHEN npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "U30-Z8 Failure"
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

  dimension: nu_failure {
    type: number
    label: "NU Failure"
    sql: ${TABLE}."NU Failure" ;;
  }

  dimension: u30z9_failure {
    type: number
    label: "U30-Z9 Failure"
    sql: ${TABLE}."U30-Z9 Failure" ;;
  }

  dimension: u30yc_failure {
    type: number
    label: "U30-YC Failure"
    sql: ${TABLE}."U30-YC Failure" ;;
  }

  dimension: u30z8_failure {
    type: number
    label: "U30-Z8 Failure"
    sql: ${TABLE}."U30-Z8 Failure" ;;
  }

  set: detail {
    fields: [created_date, nu_failure, u30z9_failure, u30yc_failure, u30z8_failure]
  }
}
