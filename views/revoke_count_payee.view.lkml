view: revoke_count_payee {
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
              ) AS success
          FROM
              team_product.looker_RM ti
          WHERE
              first_phase = 'REQMANDATECONFIRMATION-REVOKE'
              AND ti.type = 'REVOKE'
          GROUP BY
              DATE(ti.created_on),
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              success
          FROM
              handle_data
          WHERE
              handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'paytm' THEN success ELSE NULL END) AS "Paytm Success",
          MAX(CASE WHEN handle = 'ptaxis' THEN success ELSE NULL END) AS "ptaxis Success",
          MAX(CASE WHEN handle = 'pthdfc' THEN success ELSE NULL END) AS "pthdfc Success",
          MAX(CASE WHEN handle = 'ptsbi' THEN success ELSE NULL END) AS "ptsbi Success",
          MAX(CASE WHEN handle = 'ptyes' THEN success ELSE NULL END) AS "ptyes Success",
           -- Total Success Column
      (COALESCE(MAX(CASE WHEN handle = 'paytm' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptaxis' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'pthdfc' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptsbi' THEN success ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptyes' THEN success ELSE NULL END), 0)) AS "Total Success"
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

  dimension: ptaxis_success {
    type: number
    label: "ptaxis Success"
    sql: ${TABLE}."ptaxis Success" ;;
  }

  dimension: pthdfc_success {
    type: number
    label: "pthdfc Success"
    sql: ${TABLE}."pthdfc Success" ;;
  }

  dimension: ptsbi_success {
    type: number
    label: "ptsbi Success"
    sql: ${TABLE}."ptsbi Success" ;;
  }

  dimension: ptyes_success {
    type: number
    label: "ptyes Success"
    sql: ${TABLE}."ptyes Success" ;;
  }

  dimension: total_success {
    type: number
    label: "Total Success"
    sql: ${TABLE}."Total Success" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_success,
      ptaxis_success,
      pthdfc_success,
      ptsbi_success,
      ptyes_success,
      total_success
    ]
  }
}
