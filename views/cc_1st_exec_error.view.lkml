view: cc_1st_exec_error {
  derived_table: {
    sql: WITH base_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.status,
              ti.npci_resp_code
          FROM
              hive.switch.txn_info_snapshot_v3 ti
              JOIN hive.switch.txn_participants_snapshot_v3 tp
                  ON ti.txn_id = tp.txn_id
          WHERE
              ti.business_type = 'MANDATE'
              AND ti.type = 'COLLECT'
              AND tp.account_type = 'CREDIT'
              and cast(
          replace(
            json_query(
              ti.extended_info,
              'strict $.MANDATE_EXECUTION_NUMBER'
            ),
            '"',
            ''
          ) as integer
        )=1
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND tp.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -100,CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
      ),
      total_counts AS (
          SELECT
              created_date,
              COUNT(*) AS total_count
          FROM base_data
          WHERE status IN ('SUCCESS','FAILURE')
          GROUP BY created_date
      ),
      error_data AS (
          SELECT
              b.created_date,
              b.npci_resp_code,
              COUNT(*) AS error_count,
              ROUND(COUNT(*) * 100.0 / tc.total_count, 2) AS error_percentage
          FROM base_data b
          JOIN total_counts tc
              ON b.created_date = tc.created_date
          WHERE b.status = 'FAILURE'
          GROUP BY b.created_date, b.npci_resp_code, tc.total_count
      )
      SELECT
          created_date AS "Date",
          npci_resp_code AS "Response Code",
          error_count,
          CONCAT(CAST(error_percentage as VARCHAR), '%') AS "Contribution to Failure (%)"
      FROM error_data
      ORDER BY created_date DESC, error_percentage DESC
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    sql: ${TABLE}."Date" ;;
  }

  dimension: response_code {
    type: string
    label: "Response Code"
    sql: ${TABLE}."Response Code" ;;
  }

  dimension: error_count {
    type: number
    sql: ${TABLE}.error_count ;;
  }

  dimension: contribution_to_failure_ {
    type: string
    label: "Contribution to Failure (%)"
    sql: ${TABLE}."Contribution to Failure (%)" ;;
  }

  set: detail {
    fields: [date, response_code, error_count, contribution_to_failure_]
  }
}
