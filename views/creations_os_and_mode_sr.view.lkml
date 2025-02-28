view: creations_os_and_mode_sr {
  derived_table: {
    sql: WITH handle_data AS (
    SELECT
        DATE(ti.created_on) AS created_date,
        REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') AS os_app,
        REPLACE(JSON_QUERY(ti.extended_info, 'strict$.initiationMode'), '"', '') AS initiation_mode,
        CONCAT(
            CAST(
                ROUND(
                    COUNT(DISTINCT CASE WHEN status = 'SUCCESS' THEN umn ELSE NULL END) * 100.0 /
                    NULLIF(COUNT(DISTINCT umn), 0),
                2) AS VARCHAR
            ), '%'
        ) AS sr
    FROM hive.switch.txn_info_snapshot_v3 ti
    WHERE
        ti.business_type = 'MANDATE'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
        AND ti.type = 'CREATE'
    GROUP BY 1, 2, 3
),
os_wise_sr AS (
    SELECT
        created_date,
        -- Android Success Rate (SR)
        MAX(CASE WHEN os_app = 'android' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "Android_Collect_SR",
        MAX(CASE WHEN os_app = 'android' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "Android_Intent_SR",
        MAX(CASE WHEN os_app = 'android' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "Android_QR_SR",

        -- iOS Success Rate (SR)
        MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "iOS_Collect_SR",
        MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "iOS_Intent_SR",
        MAX(CASE WHEN os_app LIKE 'iOS%' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "iOS_QR_SR"
    FROM handle_data
    GROUP BY created_date
),
mode_wise_sr AS (
    SELECT
        created_date,
        AVG(CASE WHEN initiation_mode = '00' THEN sr ELSE NULL END) AS "Collect_SR",
        AVG(CASE WHEN initiation_mode = '04' THEN sr ELSE NULL END) AS "Intent_SR",
        AVG(CASE WHEN initiation_mode = '13' THEN sr ELSE NULL END) AS "QR_SR"
    FROM handle_data
    GROUP BY created_date
)
SELECT
    o.*,
    m."Collect_SR",
    m."Intent_SR",
    m."QR_SR"
FROM os_wise_sr o
JOIN mode_wise_sr m ON o.created_date = m.created_date
ORDER BY o.created_date DESC

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

  dimension: android_collect_sr {
    type: string
    sql: ${TABLE}.Android_Collect_SR ;;
  }

  dimension: android_intent_sr {
    type: string
    sql: ${TABLE}.Android_Intent_SR ;;
  }

  dimension: android_qr_sr {
    type: string
    sql: ${TABLE}.Android_QR_SR ;;
  }

  dimension: i_os_collect_sr {
    type: string
    sql: ${TABLE}.iOS_Collect_SR ;;
  }

  dimension: i_os_intent_sr {
    type: string
    sql: ${TABLE}.iOS_Intent_SR ;;
  }

  dimension: i_os_qr_sr {
    type: string
    sql: ${TABLE}.iOS_QR_SR ;;
  }

  dimension: collect_sr {
    type: string
    sql: ${TABLE}.Collect_SR ;;
  }

  dimension: intent_sr {
    type: string
    sql: ${TABLE}.Intent_SR ;;
  }

  dimension: qr_sr {
    type: string
    sql: ${TABLE}.QR_SR ;;
  }

  set: detail {
    fields: [
      created_date,
      android_collect_sr,
      android_intent_sr,
      android_qr_sr,
      i_os_collect_sr,
      i_os_intent_sr,
      i_os_qr_sr,
      collect_sr,
      intent_sr,
      qr_sr
    ]
  }
}
