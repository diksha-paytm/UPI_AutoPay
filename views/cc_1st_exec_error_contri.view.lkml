view: cc_1st_exec_error_contri {
  derived_table: {
    sql: WITH txn_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.status,
              COALESCE(NULLIF(ti.npci_resp_code, ''), 'NULL') AS npci_resp_code,  -- Handling blank response codes
              COUNT(DISTINCT CONCAT(
                  umn,
                  REPLACE(
                      JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                      '"',
                      ''
                  )
              )) AS txn_count
          FROM hive.switch.txn_info_snapshot_v3 ti
          JOIN hive.switch.txn_participants_snapshot_v3 tp
              ON ti.txn_id = tp.txn_id
          JOIN hive.switch.txn_participants_snapshot_v3 tp1
              ON ti.txn_id = tp1.txn_id
          WHERE
              ti.business_type = 'MANDATE'
              AND tp.account_type = 'CREDIT'
              AND CAST(
                  REPLACE(
                      JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                      '"',
                      ''
                  ) AS INTEGER
              ) = 1
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp1.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp.participant_type = 'PAYER'
              AND tp1.participant_type = 'PAYEE'
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.type = 'COLLECT'
          GROUP BY 1, 2, 3
      ),
      aggregated_data AS (
          SELECT
              created_date,
              CAST(SUM(CASE WHEN status = 'SUCCESS' THEN txn_count ELSE 0 END) AS BIGINT) AS success_count,
              CAST(SUM(CASE WHEN status = 'FAILURE' THEN txn_count ELSE 0 END) AS BIGINT) AS failure_count,
              CAST(SUM(CASE WHEN status IN ('FAILURE', 'SUCCESS') THEN txn_count ELSE 0 END) AS BIGINT) AS total_count
          FROM txn_data
          GROUP BY 1
      ),
      error_codes AS (
          SELECT
              created_date,
              npci_resp_code,
              SUM(txn_count) AS failure_count
          FROM txn_data
          WHERE status = 'FAILURE'
          GROUP BY 1, 2
      ),
      top_10_errors AS (
          SELECT
              e.created_date,
              e.npci_resp_code,
              CAST(ROUND(100.0 * e.failure_count / NULLIF(a.total_count, 0), 2) AS VARCHAR) || '%' AS failure_percentage,
              ROW_NUMBER() OVER (PARTITION BY e.created_date ORDER BY e.failure_count DESC) AS error_rank
          FROM error_codes e
          JOIN aggregated_data a
              ON e.created_date = a.created_date
      )
      SELECT
          created_date,
          npci_resp_code AS "Error Code",
          failure_percentage AS "Failure Contribution"
      FROM top_10_errors
      WHERE error_rank <= 10
      ORDER BY created_date DESC, failure_percentage DESC
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

  dimension: error_code {
    type: string
    label: "Error Code"
    sql: ${TABLE}."Error Code" ;;
  }

  dimension: failure_contribution {
    type: string
    label: "Failure Contribution"
    sql: ${TABLE}."Failure Contribution" ;;
  }

  set: detail {
    fields: [created_date, error_code, failure_contribution]
  }
}
