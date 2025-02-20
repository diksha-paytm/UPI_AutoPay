view: sbmd_1st_exec_count {
  derived_table: {
    sql: WITH txn_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.status,
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
              AND CAST(
                  REPLACE(
                      JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                      '"',
                      ''
                  ) AS INTEGER
              ) = 1
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"76"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp1.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND tp.participant_type = 'PAYER'
              AND tp1.participant_type = 'PAYEE'
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.type = 'COLLECT'
          GROUP BY 1, 2
      ),
      aggregated_data AS (
          SELECT
              created_date,
              CAST(SUM(CASE WHEN status = 'SUCCESS' THEN txn_count ELSE 0 END) AS BIGINT) AS success_count,
              CAST(SUM(CASE WHEN status = 'FAILURE' THEN txn_count ELSE 0 END) AS BIGINT) AS failure_count,
              CAST(SUM(CASE WHEN status IN ('FAILURE', 'SUCCESS') THEN txn_count ELSE 0 END) AS BIGINT) AS total_count
          FROM txn_data
          GROUP BY 1
      )
      SELECT
          created_date,
          success_count AS "Success",
          failure_count AS "Failure",
          total_count AS "Total",
          CAST(ROUND(100.0 * success_count / NULLIF(total_count, 0), 2) AS VARCHAR) || '%' AS "SR%"
      FROM aggregated_data
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

  dimension: success {
    type: number
    sql: ${TABLE}.Success ;;
  }

  dimension: failure {
    type: number
    sql: ${TABLE}.Failure ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.Total ;;
  }

  dimension: sr {
    type: string
    sql: ${TABLE}."SR%" ;;
  }

  set: detail {
    fields: [created_date, success, failure, total, sr]
  }
}
