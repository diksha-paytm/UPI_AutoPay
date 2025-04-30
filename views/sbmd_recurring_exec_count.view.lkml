view: sbmd_recurring_exec_count {
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
          FROM team_product.looker_RM_SBMD ti
          WHERE
              CAST(
                  REPLACE(
                      JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                      '"',
                      ''
                  ) AS INTEGER
              ) > 1
              AND ti.type = 'COLLECT'
          GROUP BY 1, 2
      ),
      aggregated_data AS (
          SELECT
              created_date,
              CAST(SUM(CASE WHEN status = 'SUCCESS' THEN txn_count ELSE 0 END) AS BIGINT) AS success_count,
              CAST(SUM(CASE WHEN status = 'FAILURE' THEN txn_count ELSE 0 END) AS BIGINT) AS failure_count,
              CAST(SUM(txn_count) AS BIGINT) AS total_count
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
