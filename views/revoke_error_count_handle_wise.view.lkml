view: revoke_error_count_handle_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              ti.npci_resp_code,
              COUNT(DISTINCT ti.txn_id) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'REVOKE'
              AND ti.status = 'FAILURE'
          GROUP BY
              DATE(ti.created_on),
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1),
              ti.npci_resp_code
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              npci_resp_code,
              failure
          FROM
              handle_data
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'UM8-QA' THEN failure ELSE NULL END) AS "Paytm UM8-QA Failure",
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "PAYTM UM8-ZM Failure",
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "PAYTM UM9 Failure",
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'UO8' THEN failure ELSE NULL END) AS "PAYTM UO8 Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "PTAXIS UM8-ZM Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "PTAXIS UM9 Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "pthdfc UM8-ZM Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "pthdfc UM9 Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptsbi UM8-ZM Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "ptsbi UM9 Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'UM8-ZM' THEN failure ELSE NULL END) AS "ptyes UM8-ZM Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'UM9' THEN failure ELSE NULL END) AS "ptyes UM9 Failure"
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

  dimension: paytm_um8qa_failure {
    type: number
    label: "Paytm UM8-QA Failure"
    sql: ${TABLE}."Paytm UM8-QA Failure" ;;
  }

  dimension: paytm_um8zm_failure {
    type: number
    label: "PAYTM UM8-ZM Failure"
    sql: ${TABLE}."PAYTM UM8-ZM Failure" ;;
  }

  dimension: paytm_um9_failure {
    type: number
    label: "PAYTM UM9 Failure"
    sql: ${TABLE}."PAYTM UM9 Failure" ;;
  }

  dimension: paytm_uo8_failure {
    type: number
    label: "PAYTM UO8 Failure"
    sql: ${TABLE}."PAYTM UO8 Failure" ;;
  }

  dimension: ptaxis_um8zm_failure {
    type: number
    label: "PTAXIS UM8-ZM Failure"
    sql: ${TABLE}."PTAXIS UM8-ZM Failure" ;;
  }

  dimension: ptaxis_um9_failure {
    type: number
    label: "PTAXIS UM9 Failure"
    sql: ${TABLE}."PTAXIS UM9 Failure" ;;
  }

  dimension: pthdfc_um8zm_failure {
    type: number
    label: "pthdfc UM8-ZM Failure"
    sql: ${TABLE}."pthdfc UM8-ZM Failure" ;;
  }

  dimension: pthdfc_um9_failure {
    type: number
    label: "pthdfc UM9 Failure"
    sql: ${TABLE}."pthdfc UM9 Failure" ;;
  }

  dimension: ptsbi_um8zm_failure {
    type: number
    label: "ptsbi UM8-ZM Failure"
    sql: ${TABLE}."ptsbi UM8-ZM Failure" ;;
  }

  dimension: ptsbi_um9_failure {
    type: number
    label: "ptsbi UM9 Failure"
    sql: ${TABLE}."ptsbi UM9 Failure" ;;
  }

  dimension: ptyes_um8zm_failure {
    type: number
    label: "ptyes UM8-ZM Failure"
    sql: ${TABLE}."ptyes UM8-ZM Failure" ;;
  }

  dimension: ptyes_um9_failure {
    type: number
    label: "ptyes UM9 Failure"
    sql: ${TABLE}."ptyes UM9 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_um8qa_failure,
      paytm_um8zm_failure,
      paytm_um9_failure,
      paytm_uo8_failure,
      ptaxis_um8zm_failure,
      ptaxis_um9_failure,
      pthdfc_um8zm_failure,
      pthdfc_um9_failure,
      ptsbi_um8zm_failure,
      ptsbi_um9_failure,
      ptyes_um8zm_failure,
      ptyes_um9_failure
    ]
  }
}
