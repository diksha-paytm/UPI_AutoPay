view: creations_os_wise_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict$.payerOsApp'), '"', '') AS Os_App,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              team_product.looker_RM ti
          WHERE
              ti.type = 'CREATE'
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
          MAX(CASE WHEN Os_App = 'android' THEN success ELSE NULL END) AS "Android Success",
          MAX(CASE WHEN Os_App = 'android' THEN failure ELSE NULL END) AS "Android Failure",
          MAX(CASE WHEN Os_App LIKE 'iOS%' THEN success ELSE NULL END) AS "iOS Success",
          MAX(CASE WHEN Os_App LIKE 'iOS%' THEN failure ELSE NULL END) AS "iOS Failure",
          COALESCE(SUM(success), 0) AS "Total Success",
          COALESCE(SUM(failure), 0) AS "Total Failure"
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

  dimension: i_os_success {
    type: number
    label: "iOS Success"
    sql: ${TABLE}."iOS Success" ;;
  }

  dimension: i_os_failure {
    type: number
    label: "iOS Failure"
    sql: ${TABLE}."iOS Failure" ;;
  }

  dimension: total_success {
    type: number
    label: "Total Success"
    sql: ${TABLE}."Total Success" ;;
  }

  dimension: total_failure {
    type: number
    label: "Total Failure"
    sql: ${TABLE}."Total Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      android_success,
      android_failure,
      i_os_success,
      i_os_failure,
      total_success,
      total_failure
    ]
  }
}
