view: sbmd {
  derived_table: {
    sql: WITH pivoted_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.type,
              ti.status,
              COUNT(DISTINCT ti.umn) AS count_umn
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"76"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY 1, 2, 3
      )

      SELECT
      created_date,

      -- CREATE type
      MAX(CASE WHEN type = 'CREATE' AND status = 'SUCCESS' THEN count_umn ELSE NULL END) AS "CREATE_SUCCESS",
      MAX(CASE WHEN type = 'CREATE' AND status = 'FAILURE' THEN count_umn ELSE NULL END) AS "CREATE_FAILURE",

      -- REVOKE type
      MAX(CASE WHEN type = 'REVOKE' AND status = 'SUCCESS' THEN count_umn ELSE NULL END) AS "REVOKE_SUCCESS",
      MAX(CASE WHEN type = 'REVOKE' AND status = 'FAILURE' THEN count_umn ELSE NULL END) AS "REVOKE_FAILURE",

      -- PAUSE type
      MAX(CASE WHEN type = 'PAUSE' AND status = 'SUCCESS' THEN count_umn ELSE NULL END) AS "PAUSE_SUCCESS",
      MAX(CASE WHEN type = 'PAUSE' AND status = 'FAILURE' THEN count_umn ELSE NULL END) AS "PAUSE_FAILURE",

      -- RESUME type
      MAX(CASE WHEN type = 'RESUME' AND status = 'SUCCESS' THEN count_umn ELSE NULL END) AS "RESUME_SUCCESS",
      MAX(CASE WHEN type = 'RESUME' AND status = 'FAILURE' THEN count_umn ELSE NULL END) AS "RESUME_FAILURE"

      FROM pivoted_data
      GROUP BY created_date
      ORDER BY created_date desc
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

  dimension: create_success {
    type: number
    sql: ${TABLE}.CREATE_SUCCESS ;;
  }

  dimension: create_failure {
    type: number
    sql: ${TABLE}.CREATE_FAILURE ;;
  }

  dimension: revoke_success {
    type: number
    sql: ${TABLE}.REVOKE_SUCCESS ;;
  }

  dimension: revoke_failure {
    type: number
    sql: ${TABLE}.REVOKE_FAILURE ;;
  }

  dimension: pause_success {
    type: number
    sql: ${TABLE}.PAUSE_SUCCESS ;;
  }

  dimension: pause_failure {
    type: number
    sql: ${TABLE}.PAUSE_FAILURE ;;
  }

  dimension: resume_success {
    type: number
    sql: ${TABLE}.RESUME_SUCCESS ;;
  }

  dimension: resume_failure {
    type: number
    sql: ${TABLE}.RESUME_FAILURE ;;
  }

  set: detail {
    fields: [
      created_date,
      create_success,
      create_failure,
      revoke_success,
      revoke_failure,
      pause_success,
      pause_failure,
      resume_success,
      resume_failure
    ]
  }
}
