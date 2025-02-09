view: cc_summary {
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
          AND tp.account_type = 'CREDIT'
          AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
          AND ti.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
          AND tp.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
          AND tp1.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
          AND tp.participant_type = 'PAYER'
          AND tp1.participant_type = 'PAYEE'
          AND ti.created_on >= CAST(DATE_ADD('day', -50,CURRENT_DATE) AS TIMESTAMP)
          AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
      ),
      aggregated_data AS (
        SELECT
          created_date,
          -- CREATE
          COUNT(CASE WHEN type = 'CREATE' AND status = 'SUCCESS' THEN 1 END) AS create_success,
          COUNT(CASE WHEN type = 'CREATE' AND status = 'FAILURE' THEN 1 END) AS create_failure,
          COUNT(CASE WHEN type = 'CREATE' AND status IN ('FAILURE','SUCCESS') THEN 1 END) AS create_total,
          CASE
            WHEN COUNT(CASE WHEN type = 'CREATE' AND status IN ('FAILURE','SUCCESS')  THEN 1 END) > 0
            THEN ROUND(
              COUNT(CASE WHEN type = 'CREATE' AND status = 'SUCCESS' THEN 1 END) * 100.0 /
              COUNT(CASE WHEN type = 'CREATE' AND status IN ('FAILURE','SUCCESS') THEN 1 END), 2
            )
            ELSE 0
          END AS create_sr,
          -- COLLECT
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status = 'SUCCESS' THEN 1 END) AS First_Exec_success,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status = 'FAILURE' THEN 1 END) AS First_Exec_failure,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status IN ('FAILURE','SUCCESS') THEN 1 END) AS First_Exec_total,
          CASE
            WHEN COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status IN ('FAILURE','SUCCESS')  THEN 1 END) > 0
            THEN ROUND(
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status = 'SUCCESS' THEN 1 END) * 100.0 /
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no = 1 AND status IN ('FAILURE','SUCCESS') THEN 1 END), 2
            )
            ELSE 0
          END AS First_Exec_SR,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status = 'SUCCESS' THEN 1 END) AS Recurring_Exec_success,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status = 'FAILURE' THEN 1 END) AS Recurring_Exec_failure,
          COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status IN ('FAILURE','SUCCESS') THEN 1 END) AS Recurring_Exec_total,
          CASE
            WHEN COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status IN ('FAILURE','SUCCESS')  THEN 1 END) > 0
            THEN ROUND(
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status = 'SUCCESS' THEN 1 END) * 100.0 /
              COUNT(CASE WHEN type = 'COLLECT' AND execution_no > 1 AND status IN ('FAILURE','SUCCESS') THEN 1 END), 2
            )
            ELSE 0
          END AS Recurring_Exec_SR,
          -- REVOKE
          COUNT(CASE WHEN type = 'REVOKE' AND status = 'SUCCESS' THEN 1 END) AS revoke_success,
          COUNT(CASE WHEN type = 'REVOKE' AND status = 'FAILURE' THEN 1 END) AS revoke_failure,
          COUNT(CASE WHEN type = 'REVOKE' AND status IN ('FAILURE','SUCCESS') THEN 1 END) AS revoke_total,
          CASE
            WHEN COUNT(CASE WHEN type = 'REVOKE' AND status IN ('FAILURE','SUCCESS') THEN 1 END) > 0
            THEN ROUND(
              COUNT(CASE WHEN type = 'REVOKE' AND status = 'SUCCESS' THEN 1 END) * 100.0 /
              COUNT(CASE WHEN type = 'REVOKE' AND status IN ('FAILURE','SUCCESS') THEN 1 END), 2
            )
            ELSE 0
          END AS revoke_sr
        FROM base_data
        GROUP BY created_date
      )
      SELECT
        created_date,
        create_success AS "CREATE-Success",
        create_failure AS "CREATE-Failure",
        create_total AS "CREATE-Total",
        CONCAT(CAST(create_sr AS VARCHAR), '%') AS "CREATE-SR",
        First_Exec_success AS "1stEXEC-Success",
        First_Exec_failure AS "1stEXEC-Failure",
        First_Exec_total AS "1stEXEC-Total",
        CONCAT(CAST(First_Exec_SR AS VARCHAR), '%') AS "1stEXEC-SR",
        Recurring_Exec_success AS "RecurringEXEC-Success",
        Recurring_Exec_failure AS "RecurringEXEC-Failure",
        Recurring_Exec_total AS "RecurringEXEC-Total",
        CONCAT(CAST(Recurring_Exec_SR AS VARCHAR), '%') AS "RecurringEXEC-SR",
        revoke_success AS "REVOKE-Success",
        revoke_failure AS "REVOKE-Failure",
        revoke_total AS "REVOKE-Total",
        CONCAT(CAST(revoke_sr AS VARCHAR), '%') AS "REVOKE-SR"
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

  dimension: createfailure {
    type: number
    sql: ${TABLE}."CREATE-Failure" ;;
  }

  dimension: createtotal {
    type: number
    sql: ${TABLE}."CREATE-Total" ;;
  }

  dimension: createsr {
    type: string
    sql: ${TABLE}."CREATE-SR" ;;
  }

  dimension: 1st_execsuccess {
    type: number
    sql: ${TABLE}."1stEXEC-Success" ;;
  }

  dimension: 1st_execfailure {
    type: number
    sql: ${TABLE}."1stEXEC-Failure" ;;
  }

  dimension: 1st_exectotal {
    type: number
    sql: ${TABLE}."1stEXEC-Total" ;;
  }

  dimension: 1st_execsr {
    type: string
    sql: ${TABLE}."1stEXEC-SR" ;;
  }

  dimension: recurring_execsuccess {
    type: number
    sql: ${TABLE}."RecurringEXEC-Success" ;;
  }

  dimension: recurring_execfailure {
    type: number
    sql: ${TABLE}."RecurringEXEC-Failure" ;;
  }

  dimension: recurring_exectotal {
    type: number
    sql: ${TABLE}."RecurringEXEC-Total" ;;
  }

  dimension: recurring_execsr {
    type: string
    sql: ${TABLE}."RecurringEXEC-SR" ;;
  }

  dimension: revokesuccess {
    type: number
    sql: ${TABLE}."REVOKE-Success" ;;
  }

  dimension: revokefailure {
    type: number
    sql: ${TABLE}."REVOKE-Failure" ;;
  }

  dimension: revoketotal {
    type: number
    sql: ${TABLE}."REVOKE-Total" ;;
  }

  dimension: revokesr {
    type: string
    sql: ${TABLE}."REVOKE-SR" ;;
  }

  set: detail {
    fields: [
      created_date,
      createsuccess,
      createfailure,
      createtotal,
      createsr,
      1st_execsuccess,
      1st_execfailure,
      1st_exectotal,
      1st_execsr,
      recurring_execsuccess,
      recurring_execfailure,
      recurring_exectotal,
      recurring_execsr,
      revokesuccess,
      revokefailure,
      revoketotal,
      revokesr
    ]
  }
}
