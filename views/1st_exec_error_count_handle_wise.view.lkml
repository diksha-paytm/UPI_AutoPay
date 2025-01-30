view: 1st_exec_error_count_handle_wise {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              ti.npci_resp_code,
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'FAILURE' THEN CONCAT(
                          ti.umn,
                          REPLACE(
                              JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                              '"',
                              ''
                          )
                      )
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
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
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'NU' THEN failure ELSE NULL END) AS "Paytm NU Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptaxis U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'U30-YC' THEN failure ELSE NULL END) AS "ptaxis U30-YC Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "pthdfc U30-Z9 Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'U30-YC' THEN failure ELSE NULL END) AS "pthdfc U30-YC Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptsbi U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "ptsbi U30-Z8 Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptyes U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "ptyes U30-Z8 Failure"
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

  dimension: paytm_nu_failure {
    type: number
    label: "Paytm NU Failure"
    sql: ${TABLE}."Paytm NU Failure" ;;
  }

  dimension: ptaxis_u30z9_failure {
    type: number
    label: "ptaxis U30-Z9 Failure"
    sql: ${TABLE}."ptaxis U30-Z9 Failure" ;;
  }

  dimension: ptaxis_u30yc_failure {
    type: number
    label: "ptaxis U30-YC Failure"
    sql: ${TABLE}."ptaxis U30-YC Failure" ;;
  }

  dimension: pthdfc_u30z9_failure {
    type: number
    label: "pthdfc U30-Z9 Failure"
    sql: ${TABLE}."pthdfc U30-Z9 Failure" ;;
  }

  dimension: pthdfc_u30yc_failure {
    type: number
    label: "pthdfc U30-YC Failure"
    sql: ${TABLE}."pthdfc U30-YC Failure" ;;
  }

  dimension: ptsbi_u30z9_failure {
    type: number
    label: "ptsbi U30-Z9 Failure"
    sql: ${TABLE}."ptsbi U30-Z9 Failure" ;;
  }

  dimension: ptsbi_u30z8_failure {
    type: number
    label: "ptsbi U30-Z8 Failure"
    sql: ${TABLE}."ptsbi U30-Z8 Failure" ;;
  }

  dimension: ptyes_u30z9_failure {
    type: number
    label: "ptyes U30-Z9 Failure"
    sql: ${TABLE}."ptyes U30-Z9 Failure" ;;
  }

  dimension: ptyes_u30z8_failure {
    type: number
    label: "ptyes U30-Z8 Failure"
    sql: ${TABLE}."ptyes U30-Z8 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_nu_failure,
      ptaxis_u30z9_failure,
      ptaxis_u30yc_failure,
      pthdfc_u30z9_failure,
      pthdfc_u30yc_failure,
      ptsbi_u30z9_failure,
      ptsbi_u30z8_failure,
      ptyes_u30z9_failure,
      ptyes_u30z8_failure
    ]
  }
}
