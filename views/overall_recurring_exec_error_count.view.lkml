view: overall_recurring_exec_error_count {
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
              AND ti.dl_last_updated IS NOT NULL
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
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
          MAX(CASE WHEN npci_resp_code = 'U30-B3' THEN failure ELSE NULL END) AS "U30-B3 Failure",
          MAX(CASE WHEN npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "U30-Z9 Failure",
          MAX(CASE WHEN npci_resp_code = 'U67-UT' THEN failure ELSE NULL END) AS "U67-UT Failure",
          MAX(CASE WHEN npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "U30-Z8 Failure",
          MAX(CASE WHEN npci_resp_code = 'U28' THEN failure ELSE NULL END) AS "U28 Failure"
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

  dimension: u30b3_failure {
    type: number
    label: "U30-B3 Failure"
    sql: ${TABLE}."U30-B3 Failure" ;;
  }

  dimension: u30z9_failure {
    type: number
    label: "U30-Z9 Failure"
    sql: ${TABLE}."U30-Z9 Failure" ;;
  }

  dimension: u67ut_failure {
    type: number
    label: "U67-UT Failure"
    sql: ${TABLE}."U67-UT Failure" ;;
  }

  dimension: u30z8_failure {
    type: number
    label: "U30-Z8 Failure"
    sql: ${TABLE}."U30-Z8 Failure" ;;
  }

  dimension: u28_failure {
    type: number
    label: "U28 Failure"
    sql: ${TABLE}."U28 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      u30b3_failure,
      u30z9_failure,
      u67ut_failure,
      u30z8_failure,
      u28_failure
    ]
  }
}
