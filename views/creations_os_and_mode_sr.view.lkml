view: creations_os_and_mode_sr {
  derived_table: {
    sql: WITH creations_os_and_mode_sr AS (
          WITH handle_data AS (
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
                  failure,
                  -- Cast to VARCHAR and add '%' for proper SR formatting
                  CONCAT(CAST(ROUND(success * 100.0 / NULLIF(success + failure, 0), 2) AS VARCHAR), '%') AS sr
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

      -- Android Success Rate (SR) with % format
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "Android_Collect_SR",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "Android_Intent_SR",
      MAX(CASE WHEN os_app = 'android' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "Android_QR_SR",

      -- iOS Success
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN success ELSE NULL END) AS "iOS_Collect_Success",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN success ELSE NULL END) AS "iOS_Intent_Success",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN success ELSE NULL END) AS "iOS_QR_Success",

      -- iOS Failure
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "iOS_Collect_Failure",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "iOS_Intent_Failure",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "iOS_QR_Failure",

      -- iOS Success Rate (SR) with % format
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "iOS_Collect_SR",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "iOS_Intent_SR",
      MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "iOS_QR_SR"

      FROM pivoted_data
      GROUP BY created_date
      ORDER BY created_date DESC
      )
      SELECT
      DATE_FORMAT(creations_os_and_mode_sr.created_date, '%Y-%m-%d') AS "created_date",

      -- Android SR with '%'
      creations_os_and_mode_sr.Android_Collect_SR AS "android_collect_sr",
      creations_os_and_mode_sr.Android_Intent_SR AS "android_intent_sr",
      creations_os_and_mode_sr.Android_QR_SR AS "android_qr_sr",

      -- iOS SR with '%'
      creations_os_and_mode_sr.iOS_Collect_SR AS "i_os_collect_sr",
      creations_os_and_mode_sr.iOS_Intent_SR AS "i_os_intent_sr",
      creations_os_and_mode_sr.iOS_QR_SR AS "i_os_qr_sr"

      FROM creations_os_and_mode_sr
      ORDER BY 1 DESC
      ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: created_date {
    type: string
    sql: ${TABLE}.created_date ;;
  }

  dimension: android_collect_sr {
    type: string
    sql: ${TABLE}.android_collect_sr ;;
  }

  dimension: android_intent_sr {
    type: string
    sql: ${TABLE}.android_intent_sr ;;
  }

  dimension: android_qr_sr {
    type: string
    sql: ${TABLE}.android_qr_sr ;;
  }

  dimension: i_os_collect_sr {
    type: string
    sql: ${TABLE}.i_os_collect_sr ;;
  }

  dimension: i_os_intent_sr {
    type: string
    sql: ${TABLE}.i_os_intent_sr ;;
  }

  dimension: i_os_qr_sr {
    type: string
    sql: ${TABLE}.i_os_qr_sr ;;
  }

  set: detail {
    fields: [
      created_date,
      android_collect_sr,
      android_intent_sr,
      android_qr_sr,
      i_os_collect_sr,
      i_os_intent_sr,
      i_os_qr_sr
    ]
  }
}
