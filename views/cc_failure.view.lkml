view: cc_failure {
  derived_table: {
    sql: WITH base_data AS (
        SELECT
          DATE(ti.created_on) AS created_date,
          ti.type,
          ti.status,
          CAST(
            REPLACE(
              JSON_QUERY(
                ti.extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            ) AS INTEGER
          ) AS execution_no
        FROM
          team_product.looker_RM_CC ti
          ),
      aggregated_data AS (
        SELECT
          created_date,
          -- CREATE
          COUNT(CASE WHEN type = 'CREATE' AND status = 'FAILURE' THEN 1 END) AS create_success,
          -- COLLECT
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status = 'FAILURE' THEN 1 END) AS First_Exec_success,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status = 'FAILURE' THEN 1 END) AS Recurring_Exec_success,
          -- REVOKE
          COUNT(CASE WHEN type = 'REVOKE' AND status = 'FAILURE' THEN 1 END) AS revoke_success
        FROM base_data
        GROUP BY created_date
      )
      SELECT
        created_date,
        create_success AS "CREATE-Success",
        First_Exec_success AS "1stEXEC-Success",
        Recurring_Exec_success AS "RecurringEXEC-Success",
        revoke_success AS "REVOKE-Success",
        -- Total Success column
          (create_success + First_Exec_success + Recurring_Exec_success + revoke_success) AS "Total-Success"
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

  dimension: createsuccess {
    type: number
    sql: ${TABLE}."CREATE-Success" ;;
  }

  dimension: 1st_execsuccess {
    type: number
    sql: ${TABLE}."1stEXEC-Success" ;;
  }

  dimension: recurring_execsuccess {
    type: number
    sql: ${TABLE}."RecurringEXEC-Success" ;;
  }

  dimension: revokesuccess {
    type: number
    sql: ${TABLE}."REVOKE-Success" ;;
  }

  dimension: totalsuccess {
    type: number
    sql: ${TABLE}."Total-Success" ;;
  }

  set: detail {
    fields: [
      created_date,
      createsuccess,
      1st_execsuccess,
      recurring_execsuccess,
      revokesuccess,
      totalsuccess
    ]
  }
}
