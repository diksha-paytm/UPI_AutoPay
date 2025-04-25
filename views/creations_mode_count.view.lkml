view: creations_mode_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              REPLACE(
                  JSON_QUERY(ti.extended_info, 'strict $.initiationMode'),
                  '"',
                  ''
              ) AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              team_product.looker_RM ti
          WHERE
              ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
              AND SUBSTRING(ti.umn, POSITION('@' IN ti.umn) + 1) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm')
          GROUP BY
              1, 2
      ),
      pivoted_data AS (
          SELECT
              created_date,
              initiation_mode,
              success,
              failure
          FROM
              handle_data
          WHERE
              initiation_mode IN ('00', '04', '13')
      )
      SELECT
          created_date,
          MAX(CASE WHEN initiation_mode = '00' THEN success ELSE NULL END) AS "Collect Success",
          MAX(CASE WHEN initiation_mode = '04' THEN success ELSE NULL END) AS "Intent Success",
          MAX(CASE WHEN initiation_mode = '13' THEN success ELSE NULL END) AS "QR Success",
          MAX(CASE WHEN initiation_mode = '00' THEN failure ELSE NULL END) AS "Collect Failure",
          MAX(CASE WHEN initiation_mode = '04' THEN failure ELSE NULL END) AS "Intent Failure",
          MAX(CASE WHEN initiation_mode = '13' THEN failure ELSE NULL END) AS "QR Failure",

      -- Total Success (Sum of all success columns)
      COALESCE(
      MAX(CASE WHEN initiation_mode = '00' THEN success ELSE NULL END), 0
      ) + COALESCE(
      MAX(CASE WHEN initiation_mode = '04' THEN success ELSE NULL END), 0
      ) + COALESCE(
      MAX(CASE WHEN initiation_mode = '13' THEN success ELSE NULL END), 0
      ) AS "Total Success",

      -- Total Failure (Sum of all failure columns)
      COALESCE(
      MAX(CASE WHEN initiation_mode = '00' THEN failure ELSE NULL END), 0
      ) + COALESCE(
      MAX(CASE WHEN initiation_mode = '04' THEN failure ELSE NULL END), 0
      ) + COALESCE(
      MAX(CASE WHEN initiation_mode = '13' THEN failure ELSE NULL END), 0
      ) AS "Total Failure"

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

  dimension: collect_success {
    type: number
    label: "Collect Success"
    sql: ${TABLE}."Collect Success" ;;
  }

  dimension: intent_success {
    type: number
    label: "Intent Success"
    sql: ${TABLE}."Intent Success" ;;
  }

  dimension: qr_success {
    type: number
    label: "QR Success"
    sql: ${TABLE}."QR Success" ;;
  }

  dimension: collect_failure {
    type: number
    label: "Collect Failure"
    sql: ${TABLE}."Collect Failure" ;;
  }

  dimension: intent_failure {
    type: number
    label: "Intent Failure"
    sql: ${TABLE}."Intent Failure" ;;
  }

  dimension: qr_failure {
    type: number
    label: "QR Failure"
    sql: ${TABLE}."QR Failure" ;;
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
      collect_success,
      intent_success,
      qr_success,
      collect_failure,
      intent_failure,
      qr_failure,
      total_success,
      total_failure
    ]
  }
}
