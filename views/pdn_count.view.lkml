view: pdn_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(fn.created_on) AS created_date,
              SUBSTRING(
          fn.txn_ref_id
          FROM
            POSITION('@' IN fn.txn_ref_id) + 1
        ) AS handle,
              COUNT(distinct
                   CASE
                      WHEN fn.status = 'SUCCESS' THEN fn.txn_ref_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(distinct
                   CASE
                      WHEN fn.status = 'FAILURE' THEN fn.txn_ref_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              team_product.looker_financial_notification fn
          WHERE
               fn.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              DATE(fn.created_on),
              SUBSTRING(
          fn.txn_ref_id
          FROM
            POSITION('@' IN fn.txn_ref_id) + 1
        )
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
          MAX(CASE WHEN handle = 'ptyes' THEN failure ELSE NULL END) AS "ptyes Failure",

      -- Total Success Column
      (COALESCE(MAX(CASE WHEN handle = 'paytm' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptaxis' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'pthdfc' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptsbi' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptyes' THEN success ELSE NULL END), 0)) AS "Total Success",
      -- Total Failure Column
      (COALESCE(MAX(CASE WHEN handle = 'paytm' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptaxis' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'pthdfc' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptsbi' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptyes' THEN failure ELSE NULL END), 0)) AS "Total Failure"
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
      paytm_success,
      paytm_failure,
      ptaxis_success,
      ptaxis_failure,
      pthdfc_success,
      pthdfc_failure,
      ptsbi_success,
      ptsbi_failure,
      ptyes_success,
      ptyes_failure,
      total_success,
      total_failure
    ]
  }
}
