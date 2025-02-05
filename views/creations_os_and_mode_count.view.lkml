view: creations_os_and_mode_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') AS os_app,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY 1, 2, 3
      ),
      pivoted_data AS (
          SELECT
              created_date,
              os_app,
              initiation_mode,
              success,
              failure
          FROM handle_data
          WHERE initiation_mode IN ('00', '04', '13')
      )
      SELECT
          created_date,

      -- Android Success
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '00' THEN success ELSE NULL END) AS "Android_Collect_Success",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '04' THEN success ELSE NULL END) AS "Android_Intent_Success",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '13' THEN success ELSE NULL END) AS "Android_QR_Success",

      -- Android Failure
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "Android_Collect_Failure",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "Android_Intent_Failure",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "Android_QR_Failure",

      -- iOS Success
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN success ELSE NULL END) AS "iOS_Collect_Success",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN success ELSE NULL END) AS "iOS_Intent_Success",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN success ELSE NULL END) AS "iOS_QR_Success",

      -- iOS Failure
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "iOS_Collect_Failure",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "iOS_Intent_Failure",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "iOS_QR_Failure"

      FROM pivoted_data
      GROUP BY created_date
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

  dimension: android_collect_success {
    type: number
    sql: ${TABLE}.Android_Collect_Success ;;
  }

  dimension: android_intent_success {
    type: number
    sql: ${TABLE}.Android_Intent_Success ;;
  }

  dimension: android_qr_success {
    type: number
    sql: ${TABLE}.Android_QR_Success ;;
  }

  dimension: android_collect_failure {
    type: number
    sql: ${TABLE}.Android_Collect_Failure ;;
  }

  dimension: android_intent_failure {
    type: number
    sql: ${TABLE}.Android_Intent_Failure ;;
  }

  dimension: android_qr_failure {
    type: number
    sql: ${TABLE}.Android_QR_Failure ;;
  }

  dimension: i_os_collect_success {
    type: number
    sql: ${TABLE}.iOS_Collect_Success ;;
  }

  dimension: i_os_intent_success {
    type: number
    sql: ${TABLE}.iOS_Intent_Success ;;
  }

  dimension: i_os_qr_success {
    type: number
    sql: ${TABLE}.iOS_QR_Success ;;
  }

  dimension: i_os_collect_failure {
    type: number
    sql: ${TABLE}.iOS_Collect_Failure ;;
  }

  dimension: i_os_intent_failure {
    type: number
    sql: ${TABLE}.iOS_Intent_Failure ;;
  }

  dimension: i_os_qr_failure {
    type: number
    sql: ${TABLE}.iOS_QR_Failure ;;
  }

  set: detail {
    fields: [
      created_date,
      android_collect_success,
      android_intent_success,
      android_qr_success,
      android_collect_failure,
      android_intent_failure,
      android_qr_failure,
      i_os_collect_success,
      i_os_intent_success,
      i_os_qr_success,
      i_os_collect_failure,
      i_os_intent_failure,
      i_os_qr_failure
    ]
  }
}
