view: payee_revoke_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'SUCCESS' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'FAILURE' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND first_phase = 'REQMANDATECONFIRMATION-REVOKE'
              AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'REVOKE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              DATE(ti.created_on),
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              success,
              failure
          FROM
              handle_data
          WHERE
              handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'paytm' THEN success ELSE NULL END) AS "Paytm Success",
          MAX(CASE WHEN handle = 'paytm' THEN failure ELSE NULL END) AS "Paytm Failure",
          MAX(CASE WHEN handle = 'ptaxis' THEN success ELSE NULL END) AS "ptaxis Success",
          MAX(CASE WHEN handle = 'ptaxis' THEN failure ELSE NULL END) AS "ptaxis Failure",
          MAX(CASE WHEN handle = 'pthdfc' THEN success ELSE NULL END) AS "pthdfc Success",
          MAX(CASE WHEN handle = 'pthdfc' THEN failure ELSE NULL END) AS "pthdfc Failure",
          MAX(CASE WHEN handle = 'ptsbi' THEN success ELSE NULL END) AS "ptsbi Success",
          MAX(CASE WHEN handle = 'ptsbi' THEN failure ELSE NULL END) AS "ptsbi Failure",
          MAX(CASE WHEN handle = 'ptyes' THEN success ELSE NULL END) AS "ptyes Success",
          MAX(CASE WHEN handle = 'ptyes' THEN failure ELSE NULL END) AS "ptyes Failure"
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

  dimension: paytm_success {
    type: number
    label: "Paytm Success"
    sql: ${TABLE}."Paytm Success" ;;
  }

  dimension: paytm_failure {
    type: number
    label: "Paytm Failure"
    sql: ${TABLE}."Paytm Failure" ;;
  }

  dimension: ptaxis_success {
    type: number
    label: "ptaxis Success"
    sql: ${TABLE}."ptaxis Success" ;;
  }

  dimension: ptaxis_failure {
    type: number
    label: "ptaxis Failure"
    sql: ${TABLE}."ptaxis Failure" ;;
  }

  dimension: pthdfc_success {
    type: number
    label: "pthdfc Success"
    sql: ${TABLE}."pthdfc Success" ;;
  }

  dimension: pthdfc_failure {
    type: number
    label: "pthdfc Failure"
    sql: ${TABLE}."pthdfc Failure" ;;
  }

  dimension: ptsbi_success {
    type: number
    label: "ptsbi Success"
    sql: ${TABLE}."ptsbi Success" ;;
  }

  dimension: ptsbi_failure {
    type: number
    label: "ptsbi Failure"
    sql: ${TABLE}."ptsbi Failure" ;;
  }

  dimension: ptyes_success {
    type: number
    label: "ptyes Success"
    sql: ${TABLE}."ptyes Success" ;;
  }

  dimension: ptyes_failure {
    type: number
    label: "ptyes Failure"
    sql: ${TABLE}."ptyes Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_success,
      paytm_failure,
      ptaxis_success,
      ptaxis_failure,
      pthdfc_success,
      pthdfc_failure,
      ptsbi_success,
      ptsbi_failure,
      ptyes_success,
      ptyes_failure
    ]
  }
}
