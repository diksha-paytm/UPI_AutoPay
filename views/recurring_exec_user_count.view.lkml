view: recurring_exec_user_count {
  derived_table: {
    sql: WITH final_status AS (
          -- Select the max status per (scopeCustId, Execution Number, Date)
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              CONCAT(
                  tp.scope_cust_id,
                  REPLACE(
                      JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                      '"',
                      ''
                  )
              ) AS combi,
          MAX_BY(ti.status, ti.created_on) AS final_status  -- Pick the highest status per Execution Number
          FROM team_product.looker_RM ti
          JOIN team_product.looker_txn_parti_RM tp
              ON ti.txn_id = tp.txn_id
          WHERE
             ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
          GROUP BY 1, 2, 3
      ),
      status_counts AS (
          -- Count distinct execution attempts per status
          SELECT
              created_date,
              handle,
              final_status,
              COUNT(DISTINCT combi) AS status_count
          FROM final_status
          WHERE final_status IN ('SUCCESS', 'FAILURE') -- Only include relevant statuses
          GROUP BY 1, 2, 3
      )
      SELECT
          sc.created_date,
          -- Success Counts
          MAX(CASE WHEN handle = 'paytm' AND final_status = 'SUCCESS' THEN status_count ELSE NULL END) AS "paytm Success",
          MAX(CASE WHEN handle = 'ptaxis' AND final_status = 'SUCCESS' THEN status_count ELSE NULL END) AS "ptaxis Success",
          MAX(CASE WHEN handle = 'pthdfc' AND final_status = 'SUCCESS' THEN status_count ELSE NULL END) AS "pthdfc Success",
          MAX(CASE WHEN handle = 'ptsbi' AND final_status = 'SUCCESS' THEN status_count ELSE NULL END) AS "ptsbi Success",
          MAX(CASE WHEN handle = 'ptyes' AND final_status = 'SUCCESS' THEN status_count ELSE NULL END) AS "ptyes Success",

      -- Failure Counts
      MAX(CASE WHEN handle = 'paytm' AND final_status = 'FAILURE' THEN status_count ELSE NULL END) AS "paytm Failure",
      MAX(CASE WHEN handle = 'ptaxis' AND final_status = 'FAILURE' THEN status_count ELSE NULL END) AS "ptaxis Failure",
      MAX(CASE WHEN handle = 'pthdfc' AND final_status = 'FAILURE' THEN status_count ELSE NULL END) AS "pthdfc Failure",
      MAX(CASE WHEN handle = 'ptsbi' AND final_status = 'FAILURE' THEN status_count ELSE NULL END) AS "ptsbi Failure",
      MAX(CASE WHEN handle = 'ptyes' AND final_status = 'FAILURE' THEN status_count ELSE NULL END) AS "ptyes Failure",

      -- Total Success & Failure Counts
      (COALESCE(MAX(CASE WHEN final_status = 'SUCCESS' THEN status_count ELSE NULL END), 0)) AS "Total Success",
      (COALESCE(MAX(CASE WHEN final_status = 'FAILURE' THEN status_count ELSE NULL END), 0)) AS "Total Failure"
      FROM status_counts sc
      GROUP BY sc.created_date
      ORDER BY sc.created_date DESC
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
    label: "paytm Success"
    sql: ${TABLE}."paytm Success" ;;
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
      ptaxis_success,
      pthdfc_success,
      ptsbi_success,
      ptyes_success,
      paytm_failure,
      ptaxis_failure,
      pthdfc_failure,
      ptsbi_failure,
      ptyes_failure,
      total_success,
      total_failure
    ]
  }
}
