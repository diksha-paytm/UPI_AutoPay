view: sbmd_failure {
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
              hive.switch.txn_info_snapshot_v3 ti
              JOIN hive.switch.txn_participants_snapshot_v3 tp
                  ON ti.txn_id = tp.txn_id
              JOIN hive.switch.txn_participants_snapshot_v3 tp1
                  ON ti.txn_id = tp1.txn_id
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"76"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
        AND t1.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -30,CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
       AND tp.participant_type = 'PAYER'
              AND tp1.participant_type = 'PAYEE'
            ),
      aggregated_data AS (
          SELECT
              created_date,
              -- CREATE
              COUNT(CASE WHEN type = 'CREATE' AND status = 'FAILURE' THEN 1 END) AS create_failure,
              -- COLLECT
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status = 'FAILURE' THEN 1 END) AS First_Exec_failure,
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status = 'FAILURE' THEN 1 END) AS Recurring_Exec_failure,
              -- REVOKE
              COUNT(CASE WHEN type = 'REVOKE' AND status = 'FAILURE' THEN 1 END) AS revoke_failure
          FROM base_data
          GROUP BY created_date
      )
      SELECT
          created_date,
          create_failure AS "CREATE-Failure",
          First_Exec_failure AS "1stEXEC-Failure",
          Recurring_Exec_failure AS "RecurringEXEC-Failure",
          revoke_failure AS "REVOKE-Failure",
          -- Total Failures column
          (create_failure + First_Exec_failure + Recurring_Exec_failure + revoke_failure) AS "Total-Failure"
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

  dimension: createfailure {
    type: number
    sql: ${TABLE}."CREATE-Failure" ;;
  }

  dimension: 1st_execfailure {
    type: number
    sql: ${TABLE}."1stEXEC-Failure" ;;
  }

  dimension: recurring_execfailure {
    type: number
    sql: ${TABLE}."RecurringEXEC-Failure" ;;
  }

  dimension: revokefailure {
    type: number
    sql: ${TABLE}."REVOKE-Failure" ;;
  }

  dimension: totalfailure {
    type: number
    sql: ${TABLE}."Total-Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      createfailure,
      1st_execfailure,
      recurring_execfailure,
      revokefailure,
      totalfailure
    ]
  }
}
