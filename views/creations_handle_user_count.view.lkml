view: creations_handle_user_count {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN tp.scope_cust_id ELSE NULL END) AS success_users,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN tp.scope_cust_id ELSE NULL END) AS failure_users
          FROM
              team_product.looker_RM ti
          JOIN
              team_product.looker_txn_parti_RM tp
              ON ti.txn_id = tp.txn_id
          WHERE
             ti.type = 'CREATE'
              AND ti.status IN ('SUCCESS', 'FAILURE')
          GROUP BY
              DATE(ti.created_on),
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              success_users,
              failure_users
          FROM
              handle_data
          WHERE
              handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
      )
      SELECT
          created_date,

      -- Success Counts at User Level
      MAX(CASE WHEN handle = 'ptaxis' THEN success_users ELSE NULL END) AS "ptaxis Success",
      MAX(CASE WHEN handle = 'pthdfc' THEN success_users ELSE NULL END) AS "pthdfc Success",
      MAX(CASE WHEN handle = 'ptsbi' THEN success_users ELSE NULL END) AS "ptsbi Success",
      MAX(CASE WHEN handle = 'ptyes' THEN success_users ELSE NULL END) AS "ptyes Success",

      -- Failure Counts at User Level (including Paytm)
      MAX(CASE WHEN handle = 'paytm' THEN failure_users ELSE NULL END) AS "paytm Failure",
      MAX(CASE WHEN handle = 'ptaxis' THEN failure_users ELSE NULL END) AS "ptaxis Failure",
      MAX(CASE WHEN handle = 'pthdfc' THEN failure_users ELSE NULL END) AS "pthdfc Failure",
      MAX(CASE WHEN handle = 'ptsbi' THEN failure_users ELSE NULL END) AS "ptsbi Failure",
      MAX(CASE WHEN handle = 'ptyes' THEN failure_users ELSE NULL END) AS "ptyes Failure",

      -- Total Success and Failure at User Level
      COALESCE(SUM(success_users), 0) AS "Total Success Users",
      COALESCE(SUM(failure_users), 0) AS "Total Failure Users"

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

  dimension: paytm_failure {
    type: number
    label: "paytm Failure"
    sql: ${TABLE}."paytm Failure" ;;
  }

  dimension: ptaxis_failure {
    type: number
    label: "ptaxis Failure"
    sql: ${TABLE}."ptaxis Failure" ;;
  }

  dimension: pthdfc_failure {
    type: number
    label: "pthdfc Failure"
    sql: ${TABLE}."pthdfc Failure" ;;
  }

  dimension: ptsbi_failure {
    type: number
    label: "ptsbi Failure"
    sql: ${TABLE}."ptsbi Failure" ;;
  }

  dimension: ptyes_failure {
    type: number
    label: "ptyes Failure"
    sql: ${TABLE}."ptyes Failure" ;;
  }

  dimension: total_success_users {
    type: number
    label: "Total Success Users"
    sql: ${TABLE}."Total Success Users" ;;
  }

  dimension: total_failure_users {
    type: number
    label: "Total Failure Users"
    sql: ${TABLE}."Total Failure Users" ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_success,
      pthdfc_success,
      ptsbi_success,
      ptyes_success,
      paytm_failure,
      ptaxis_failure,
      pthdfc_failure,
      ptsbi_failure,
      ptyes_failure,
      total_success_users,
      total_failure_users
    ]
  }
}
