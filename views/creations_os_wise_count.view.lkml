view: creations_os_wise_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') as Os_App,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              DATE(ti.created_on),
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '')
      ),
      pivoted_data AS (
          SELECT
              created_date,
              Os_App,
              success,
              failure
          FROM
              handle_data
      )
      SELECT
          created_date,
          MAX(CASE WHEN Os_App =  'android' THEN success ELSE NULL END) AS "Android Success",
          MAX(CASE WHEN Os_App = 'android' THEN failure ELSE NULL END) AS "Android Failure",
          MAX(CASE WHEN Os_App like 'iOS%' THEN failure ELSE NULL END) AS "iOS Failure",
          MAX(CASE WHEN Os_App like 'iOS%' THEN success ELSE NULL END) AS "iOS Success"
          FROM
          pivoted_data
      GROUP BY
          created_date
      ORDER BY
          created_date DESC
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

  dimension: android_success {
    type: number
    label: "Android Success"
    sql: ${TABLE}."Android Success" ;;
  }

  dimension: android_failure {
    type: number
    label: "Android Failure"
    sql: ${TABLE}."Android Failure" ;;
  }

  dimension: i_os_failure {
    type: number
    label: "iOS Failure"
    sql: ${TABLE}."iOS Failure" ;;
  }

  dimension: i_os_success {
    type: number
    label: "iOS Success"
    sql: ${TABLE}."iOS Success" ;;
  }

  set: detail {
    fields: [created_date, android_success, android_failure, i_os_failure, i_os_success]
  }
}
