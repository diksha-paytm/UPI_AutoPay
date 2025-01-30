view: recurring_exec_error_count_handle_wise {
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
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
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
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "Paytm U30-Z9 Failure",
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "Paytm U30-Z8 Failure",
          MAX(CASE WHEN handle = 'paytm' AND npci_resp_code = 'U28' THEN failure ELSE NULL END) AS "Paytm U28 Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptaxis U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptaxis' AND npci_resp_code = 'U67-UT' THEN failure ELSE NULL END) AS "ptaxis U67-UT Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "pthdfc U30-Z9 Failure",
          MAX(CASE WHEN handle = 'pthdfc' AND npci_resp_code = 'U67-UT' THEN failure ELSE NULL END) AS "pthdfc U67-UT Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptsbi U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptsbi' AND npci_resp_code = 'U30-Z8' THEN failure ELSE NULL END) AS "ptsbi U30-Z8 Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'U30-Z9' THEN failure ELSE NULL END) AS "ptyes U30-Z9 Failure",
          MAX(CASE WHEN handle = 'ptyes' AND npci_resp_code = 'U30-B3' THEN failure ELSE NULL END) AS "ptyes U30-B3 Failure"
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

  dimension: paytm_u30z9_failure {
    type: number
    label: "Paytm U30-Z9 Failure"
    sql: ${TABLE}."Paytm U30-Z9 Failure" ;;
  }

  dimension: paytm_u30z8_failure {
    type: number
    label: "Paytm U30-Z8 Failure"
    sql: ${TABLE}."Paytm U30-Z8 Failure" ;;
  }

  dimension: paytm_u28_failure {
    type: number
    label: "Paytm U28 Failure"
    sql: ${TABLE}."Paytm U28 Failure" ;;
  }

  dimension: ptaxis_u30z9_failure {
    type: number
    label: "ptaxis U30-Z9 Failure"
    sql: ${TABLE}."ptaxis U30-Z9 Failure" ;;
  }

  dimension: ptaxis_u67ut_failure {
    type: number
    label: "ptaxis U67-UT Failure"
    sql: ${TABLE}."ptaxis U67-UT Failure" ;;
  }

  dimension: pthdfc_u30z9_failure {
    type: number
    label: "pthdfc U30-Z9 Failure"
    sql: ${TABLE}."pthdfc U30-Z9 Failure" ;;
  }

  dimension: pthdfc_u67ut_failure {
    type: number
    label: "pthdfc U67-UT Failure"
    sql: ${TABLE}."pthdfc U67-UT Failure" ;;
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

  dimension: ptyes_u30b3_failure {
    type: number
    label: "ptyes U30-B3 Failure"
    sql: ${TABLE}."ptyes U30-B3 Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_u30z9_failure,
      paytm_u30z8_failure,
      paytm_u28_failure,
      ptaxis_u30z9_failure,
      ptaxis_u67ut_failure,
      pthdfc_u30z9_failure,
      pthdfc_u67ut_failure,
      ptsbi_u30z9_failure,
      ptsbi_u30z8_failure,
      ptyes_u30z9_failure,
      ptyes_u30b3_failure
    ]
  }
}
