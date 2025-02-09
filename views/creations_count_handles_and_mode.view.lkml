view: creations_count_handles_and_mode {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) AS handle,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS'
                                  AND LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes')
                                  THEN ti.umn ELSE NULL END) AS success,
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
              handle,
              initiation_mode,
              success,
              failure
          FROM handle_data
          WHERE initiation_mode IN ('00', '04', '13')
      )
      SELECT
          created_date,

      -- Success Counts
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '00' THEN success ELSE NULL END) AS "ptaxis_Collect_Success",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '04' THEN success ELSE NULL END) AS "ptaxis_Intent_Success",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '13' THEN success ELSE NULL END) AS "ptaxis_QR_Success",

      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '00' THEN success ELSE NULL END) AS "pthdfc_Collect_Success",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '04' THEN success ELSE NULL END) AS "pthdfc_Intent_Success",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '13' THEN success ELSE NULL END) AS "pthdfc_QR_Success",

      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '00' THEN success ELSE NULL END) AS "ptsbi_Collect_Success",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '04' THEN success ELSE NULL END) AS "ptsbi_Intent_Success",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '13' THEN success ELSE NULL END) AS "ptsbi_QR_Success",

      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '00' THEN success ELSE NULL END) AS "ptyes_Collect_Success",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '04' THEN success ELSE NULL END) AS "ptyes_Intent_Success",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '13' THEN success ELSE NULL END) AS "ptyes_QR_Success",

      -- Failure Counts (Including Paytm)
      MAX(CASE WHEN handle = 'paytm' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "paytm_Collect_Failure",
      MAX(CASE WHEN handle = 'paytm' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "paytm_Intent_Failure",
      MAX(CASE WHEN handle = 'paytm' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "paytm_QR_Failure",

      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "ptaxis_Collect_Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "ptaxis_Intent_Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "ptaxis_QR_Failure",

      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "pthdfc_Collect_Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "pthdfc_Intent_Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "pthdfc_QR_Failure",

      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "ptsbi_Collect_Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "ptsbi_Intent_Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "ptsbi_QR_Failure",

      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '00' THEN failure ELSE NULL END) AS "ptyes_Collect_Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '04' THEN failure ELSE NULL END) AS "ptyes_Intent_Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '13' THEN failure ELSE NULL END) AS "ptyes_QR_Failure",

      -- Mode-wise Totals
      COALESCE(SUM(CASE WHEN initiation_mode = '00' THEN success ELSE 0 END), 0) AS "Collect_Mode_Success",
      COALESCE(SUM(CASE WHEN initiation_mode = '04' THEN success ELSE 0 END), 0) AS "Intent_Mode_Success",
      COALESCE(SUM(CASE WHEN initiation_mode = '13' THEN success ELSE 0 END), 0) AS "QR_Mode_Success",

      COALESCE(SUM(CASE WHEN initiation_mode = '00' THEN failure ELSE 0 END), 0) AS "Collect_Mode_Failure",
      COALESCE(SUM(CASE WHEN initiation_mode = '04' THEN failure ELSE 0 END), 0) AS "Intent_Mode_Failure",
      COALESCE(SUM(CASE WHEN initiation_mode = '13' THEN failure ELSE 0 END), 0) AS "QR_Mode_Failure"

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

  dimension: ptaxis_collect_success {
    type: number
    sql: ${TABLE}.ptaxis_Collect_Success ;;
  }

  dimension: ptaxis_intent_success {
    type: number
    sql: ${TABLE}.ptaxis_Intent_Success ;;
  }

  dimension: ptaxis_qr_success {
    type: number
    sql: ${TABLE}.ptaxis_QR_Success ;;
  }

  dimension: pthdfc_collect_success {
    type: number
    sql: ${TABLE}.pthdfc_Collect_Success ;;
  }

  dimension: pthdfc_intent_success {
    type: number
    sql: ${TABLE}.pthdfc_Intent_Success ;;
  }

  dimension: pthdfc_qr_success {
    type: number
    sql: ${TABLE}.pthdfc_QR_Success ;;
  }

  dimension: ptsbi_collect_success {
    type: number
    sql: ${TABLE}.ptsbi_Collect_Success ;;
  }

  dimension: ptsbi_intent_success {
    type: number
    sql: ${TABLE}.ptsbi_Intent_Success ;;
  }

  dimension: ptsbi_qr_success {
    type: number
    sql: ${TABLE}.ptsbi_QR_Success ;;
  }

  dimension: ptyes_collect_success {
    type: number
    sql: ${TABLE}.ptyes_Collect_Success ;;
  }

  dimension: ptyes_intent_success {
    type: number
    sql: ${TABLE}.ptyes_Intent_Success ;;
  }

  dimension: ptyes_qr_success {
    type: number
    sql: ${TABLE}.ptyes_QR_Success ;;
  }

  dimension: paytm_collect_failure {
    type: number
    sql: ${TABLE}.paytm_Collect_Failure ;;
  }

  dimension: paytm_intent_failure {
    type: number
    sql: ${TABLE}.paytm_Intent_Failure ;;
  }

  dimension: paytm_qr_failure {
    type: number
    sql: ${TABLE}.paytm_QR_Failure ;;
  }

  dimension: ptaxis_collect_failure {
    type: number
    sql: ${TABLE}.ptaxis_Collect_Failure ;;
  }

  dimension: ptaxis_intent_failure {
    type: number
    sql: ${TABLE}.ptaxis_Intent_Failure ;;
  }

  dimension: ptaxis_qr_failure {
    type: number
    sql: ${TABLE}.ptaxis_QR_Failure ;;
  }

  dimension: pthdfc_collect_failure {
    type: number
    sql: ${TABLE}.pthdfc_Collect_Failure ;;
  }

  dimension: pthdfc_intent_failure {
    type: number
    sql: ${TABLE}.pthdfc_Intent_Failure ;;
  }

  dimension: pthdfc_qr_failure {
    type: number
    sql: ${TABLE}.pthdfc_QR_Failure ;;
  }

  dimension: ptsbi_collect_failure {
    type: number
    sql: ${TABLE}.ptsbi_Collect_Failure ;;
  }

  dimension: ptsbi_intent_failure {
    type: number
    sql: ${TABLE}.ptsbi_Intent_Failure ;;
  }

  dimension: ptsbi_qr_failure {
    type: number
    sql: ${TABLE}.ptsbi_QR_Failure ;;
  }

  dimension: ptyes_collect_failure {
    type: number
    sql: ${TABLE}.ptyes_Collect_Failure ;;
  }

  dimension: ptyes_intent_failure {
    type: number
    sql: ${TABLE}.ptyes_Intent_Failure ;;
  }

  dimension: ptyes_qr_failure {
    type: number
    sql: ${TABLE}.ptyes_QR_Failure ;;
  }

  dimension: collect_mode_success {
    type: number
    sql: ${TABLE}.Collect_Mode_Success ;;
  }

  dimension: intent_mode_success {
    type: number
    sql: ${TABLE}.Intent_Mode_Success ;;
  }

  dimension: qr_mode_success {
    type: number
    sql: ${TABLE}.QR_Mode_Success ;;
  }

  dimension: collect_mode_failure {
    type: number
    sql: ${TABLE}.Collect_Mode_Failure ;;
  }

  dimension: intent_mode_failure {
    type: number
    sql: ${TABLE}.Intent_Mode_Failure ;;
  }

  dimension: qr_mode_failure {
    type: number
    sql: ${TABLE}.QR_Mode_Failure ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_collect_success,
      ptaxis_intent_success,
      ptaxis_qr_success,
      pthdfc_collect_success,
      pthdfc_intent_success,
      pthdfc_qr_success,
      ptsbi_collect_success,
      ptsbi_intent_success,
      ptsbi_qr_success,
      ptyes_collect_success,
      ptyes_intent_success,
      ptyes_qr_success,
      paytm_collect_failure,
      paytm_intent_failure,
      paytm_qr_failure,
      ptaxis_collect_failure,
      ptaxis_intent_failure,
      ptaxis_qr_failure,
      pthdfc_collect_failure,
      pthdfc_intent_failure,
      pthdfc_qr_failure,
      ptsbi_collect_failure,
      ptsbi_intent_failure,
      ptsbi_qr_failure,
      ptyes_collect_failure,
      ptyes_intent_failure,
      ptyes_qr_failure,
      collect_mode_success,
      intent_mode_success,
      qr_mode_success,
      collect_mode_failure,
      intent_mode_failure,
      qr_mode_failure
    ]
  }
}
