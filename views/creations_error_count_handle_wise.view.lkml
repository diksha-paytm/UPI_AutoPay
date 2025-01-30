view: creations_error_count_handle_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              replace(json_query(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              ti.npci_resp_code,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure

      FROM
      hive.switch.txn_info_snapshot_v3 ti
      WHERE
      ti.business_type = 'MANDATE'
      AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
      AND ti.dl_last_updated IS NOT NULL
      AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
      AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
      AND ti.type = 'CREATE'
      AND ti.status = 'FAILURE'
      AND ti.npci_resp_code IN ('UM3', 'ZA', 'UM8-ZM', 'UM8', 'UM1', 'UM9', 'MD00', 'U66', 'UM8-Z6', 'UM2')

      GROUP BY
      1,2,3,4
      ),
      pivoted_data AS (
      SELECT
      created_date,
      handle,
      initiation_mode,
      npci_resp_code,
      failure
      FROM
      handle_data
      )
      SELECT
      created_date,
      -- ptaxis specific error codes
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '00' and npci_resp_code = 'UM3' THEN failure ELSE NULL END) AS "ptaxis collect UM3 Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '00' and npci_resp_code = 'ZA' THEN failure ELSE NULL END) AS "ptaxis collect ZA Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '00' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptaxis collect UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '04' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptaxis INTENT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '04' and npci_resp_code = 'UM8' THEN failure ELSE NULL END) AS "ptaxis INTENT UM8 Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '04' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptaxis INTENT UM1 Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '13' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptaxis QR UM1 Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '13' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptaxis QR UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode= '13' and npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "ptaxis QR UM9 Failure",
      -- pthdfc specific error codes
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '00' and npci_resp_code = 'UM3' THEN failure ELSE NULL END) AS "pthdfc COLLECT UM3 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '00' and npci_resp_code = 'MD00' THEN failure ELSE NULL END) AS "pthdfc COLLECT MD00 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '00' and npci_resp_code = 'U66' THEN failure ELSE NULL END) AS "pthdfc COLLECT U66 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '04' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "pthdfc INTENT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '04' and npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "pthdfc INTENT UM9 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '04' and npci_resp_code = 'UM8-Z6' THEN failure ELSE NULL END) AS "pthdfc INTENT UM8-Z6 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '13' and npci_resp_code = 'U66' THEN failure ELSE NULL END) AS "pthdfc QR U66 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '13' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "pthdfc QR UM1 Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode= '13' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "pthdfc QR UM8-ZM Failure",
      -- ptsbi specific error codes
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '00' and npci_resp_code = 'UM3' THEN failure ELSE NULL END) AS "ptsbi COLLECT UM3 Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '00' and npci_resp_code = 'MD00' THEN failure ELSE NULL END) AS "ptsbi COLLECT MD00 Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '00' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptsbi COLLECT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '04' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptsbi INTENT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '04' and npci_resp_code = 'UM8-Z6' THEN failure ELSE NULL END) AS "ptsbi INTENT UM8-Z6 Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '04' and npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "ptsbi INTENT UM9 Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '13' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptsbi QR UM1 Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '13' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptsbi QR UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode= '13' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptsbi QR UM2 Failure",
      -- ptyes specific error codes
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '00' and npci_resp_code = 'UM3' THEN failure ELSE NULL END) AS "ptyes COLLECT UM3 Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '00' and npci_resp_code = 'ZA' THEN failure ELSE NULL END) AS "ptyes COLLECT ZA Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '00' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptyes COLLECT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '04' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptyes INTENT UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '04' and npci_resp_code = 'UM8' THEN failure ELSE NULL END) AS "ptyes INTENT UM8 Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '04' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptyes INTENT UM1 Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '13' and npci_resp_code = 'UM1' THEN failure ELSE NULL END) AS "ptyes QR UM1 Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '13' and npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptyes QR UM8-ZM Failure",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode= '13' and npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "ptyes QR UM9 Failure"
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

  dimension: ptaxis_collect_um3_failure {
    type: number
    label: "ptaxis collect UM3 Failure"
    sql: ${TABLE}."ptaxis collect UM3 Failure" ;;
  }

  dimension: ptaxis_collect_za_failure {
    type: number
    label: "ptaxis collect ZA Failure"
    sql: ${TABLE}."ptaxis collect ZA Failure" ;;
  }

  dimension: ptaxis_collect_um8zm_failure {
    type: number
    label: "ptaxis collect UM8-ZM Failure"
    sql: ${TABLE}."ptaxis collect UM8-ZM Failure" ;;
  }

  dimension: ptaxis_intent_um8zm_failure {
    type: number
    label: "ptaxis INTENT UM8-ZM Failure"
    sql: ${TABLE}."ptaxis INTENT UM8-ZM Failure" ;;
  }

  dimension: ptaxis_intent_um8_failure {
    type: number
    label: "ptaxis INTENT UM8 Failure"
    sql: ${TABLE}."ptaxis INTENT UM8 Failure" ;;
  }

  dimension: ptaxis_intent_um1_failure {
    type: number
    label: "ptaxis INTENT UM1 Failure"
    sql: ${TABLE}."ptaxis INTENT UM1 Failure" ;;
  }

  dimension: ptaxis_qr_um1_failure {
    type: number
    label: "ptaxis QR UM1 Failure"
    sql: ${TABLE}."ptaxis QR UM1 Failure" ;;
  }

  dimension: ptaxis_qr_um8zm_failure {
    type: number
    label: "ptaxis QR UM8-ZM Failure"
    sql: ${TABLE}."ptaxis QR UM8-ZM Failure" ;;
  }

  dimension: ptaxis_qr_um9_failure {
    type: number
    label: "ptaxis QR UM9 Failure"
    sql: ${TABLE}."ptaxis QR UM9 Failure" ;;
  }

  dimension: pthdfc_collect_um3_failure {
    type: number
    label: "pthdfc COLLECT UM3 Failure"
    sql: ${TABLE}."pthdfc COLLECT UM3 Failure" ;;
  }

  dimension: pthdfc_collect_md00_failure {
    type: number
    label: "pthdfc COLLECT MD00 Failure"
    sql: ${TABLE}."pthdfc COLLECT MD00 Failure" ;;
  }

  dimension: pthdfc_collect_u66_failure {
    type: number
    label: "pthdfc COLLECT U66 Failure"
    sql: ${TABLE}."pthdfc COLLECT U66 Failure" ;;
  }

  dimension: pthdfc_intent_um8zm_failure {
    type: number
    label: "pthdfc INTENT UM8-ZM Failure"
    sql: ${TABLE}."pthdfc INTENT UM8-ZM Failure" ;;
  }

  dimension: pthdfc_intent_um9_failure {
    type: number
    label: "pthdfc INTENT UM9 Failure"
    sql: ${TABLE}."pthdfc INTENT UM9 Failure" ;;
  }

  dimension: pthdfc_intent_um8z6_failure {
    type: number
    label: "pthdfc INTENT UM8-Z6 Failure"
    sql: ${TABLE}."pthdfc INTENT UM8-Z6 Failure" ;;
  }

  dimension: pthdfc_qr_u66_failure {
    type: number
    label: "pthdfc QR U66 Failure"
    sql: ${TABLE}."pthdfc QR U66 Failure" ;;
  }

  dimension: pthdfc_qr_um1_failure {
    type: number
    label: "pthdfc QR UM1 Failure"
    sql: ${TABLE}."pthdfc QR UM1 Failure" ;;
  }

  dimension: pthdfc_qr_um8zm_failure {
    type: number
    label: "pthdfc QR UM8-ZM Failure"
    sql: ${TABLE}."pthdfc QR UM8-ZM Failure" ;;
  }

  dimension: ptsbi_collect_um3_failure {
    type: number
    label: "ptsbi COLLECT UM3 Failure"
    sql: ${TABLE}."ptsbi COLLECT UM3 Failure" ;;
  }

  dimension: ptsbi_collect_md00_failure {
    type: number
    label: "ptsbi COLLECT MD00 Failure"
    sql: ${TABLE}."ptsbi COLLECT MD00 Failure" ;;
  }

  dimension: ptsbi_collect_um8zm_failure {
    type: number
    label: "ptsbi COLLECT UM8-ZM Failure"
    sql: ${TABLE}."ptsbi COLLECT UM8-ZM Failure" ;;
  }

  dimension: ptsbi_intent_um8zm_failure {
    type: number
    label: "ptsbi INTENT UM8-ZM Failure"
    sql: ${TABLE}."ptsbi INTENT UM8-ZM Failure" ;;
  }

  dimension: ptsbi_intent_um8z6_failure {
    type: number
    label: "ptsbi INTENT UM8-Z6 Failure"
    sql: ${TABLE}."ptsbi INTENT UM8-Z6 Failure" ;;
  }

  dimension: ptsbi_intent_um9_failure {
    type: number
    label: "ptsbi INTENT UM9 Failure"
    sql: ${TABLE}."ptsbi INTENT UM9 Failure" ;;
  }

  dimension: ptsbi_qr_um1_failure {
    type: number
    label: "ptsbi QR UM1 Failure"
    sql: ${TABLE}."ptsbi QR UM1 Failure" ;;
  }

  dimension: ptsbi_qr_um8zm_failure {
    type: number
    label: "ptsbi QR UM8-ZM Failure"
    sql: ${TABLE}."ptsbi QR UM8-ZM Failure" ;;
  }

  dimension: ptsbi_qr_um2_failure {
    type: number
    label: "ptsbi QR UM2 Failure"
    sql: ${TABLE}."ptsbi QR UM2 Failure" ;;
  }

  dimension: ptyes_collect_um3_failure {
    type: number
    label: "ptyes COLLECT UM3 Failure"
    sql: ${TABLE}."ptyes COLLECT UM3 Failure" ;;
  }

  dimension: ptyes_collect_za_failure {
    type: number
    label: "ptyes COLLECT ZA Failure"
    sql: ${TABLE}."ptyes COLLECT ZA Failure" ;;
  }

  dimension: ptyes_collect_um8zm_failure {
    type: number
    label: "ptyes COLLECT UM8-ZM Failure"
    sql: ${TABLE}."ptyes COLLECT UM8-ZM Failure" ;;
  }

  dimension: ptyes_intent_um8zm_failure {
    type: number
    label: "ptyes INTENT UM8-ZM Failure"
    sql: ${TABLE}."ptyes INTENT UM8-ZM Failure" ;;
  }

  dimension: ptyes_intent_um8_failure {
    type: number
    label: "ptyes INTENT UM8 Failure"
    sql: ${TABLE}."ptyes INTENT UM8 Failure" ;;
  }

  dimension: ptyes_intent_um1_failure {
    type: number
    label: "ptyes INTENT UM1 Failure"
    sql: ${TABLE}."ptyes INTENT UM1 Failure" ;;
  }

  dimension: ptyes_qr_um1_failure {
    type: number
    label: "ptyes QR UM1 Failure"
    sql: ${TABLE}."ptyes QR UM1 Failure" ;;
  }

  dimension: ptyes_qr_um8zm_failure {
    type: number
    label: "ptyes QR UM8-ZM Failure"
    sql: ${TABLE}."ptyes QR UM8-ZM Failure" ;;
  }

  dimension: ptyes_qr_um9_failure {
    type: number
    label: "ptyes QR UM9 Failure"
    sql: ${TABLE}."ptyes QR UM9 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_collect_um3_failure,
      ptaxis_collect_za_failure,
      ptaxis_collect_um8zm_failure,
      ptaxis_intent_um8zm_failure,
      ptaxis_intent_um8_failure,
      ptaxis_intent_um1_failure,
      ptaxis_qr_um1_failure,
      ptaxis_qr_um8zm_failure,
      ptaxis_qr_um9_failure,
      pthdfc_collect_um3_failure,
      pthdfc_collect_md00_failure,
      pthdfc_collect_u66_failure,
      pthdfc_intent_um8zm_failure,
      pthdfc_intent_um9_failure,
      pthdfc_intent_um8z6_failure,
      pthdfc_qr_u66_failure,
      pthdfc_qr_um1_failure,
      pthdfc_qr_um8zm_failure,
      ptsbi_collect_um3_failure,
      ptsbi_collect_md00_failure,
      ptsbi_collect_um8zm_failure,
      ptsbi_intent_um8zm_failure,
      ptsbi_intent_um8z6_failure,
      ptsbi_intent_um9_failure,
      ptsbi_qr_um1_failure,
      ptsbi_qr_um8zm_failure,
      ptsbi_qr_um2_failure,
      ptyes_collect_um3_failure,
      ptyes_collect_za_failure,
      ptyes_collect_um8zm_failure,
      ptyes_intent_um8zm_failure,
      ptyes_intent_um8_failure,
      ptyes_intent_um1_failure,
      ptyes_qr_um1_failure,
      ptyes_qr_um8zm_failure,
      ptyes_qr_um9_failure
    ]
  }
}
