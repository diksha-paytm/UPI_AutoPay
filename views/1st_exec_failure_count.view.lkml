view: 1st_exec_failure_count {
  derived_table: {
    sql: WITH final_status AS (
          -- Select the max status per (UMN, Execution Number, Date)
          SELECT
              DATE(ti.created_on) AS created_date,
               SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
             concat(
            umn,
            replace(
              json_query(
                extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            )
          ) as combi,
           MAX(ti.status) AS final_status  -- ✅ Pick the highest status per UMN, Exec Number, Date
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
          GROUP BY 1, 2 ,3 -- ✅ Ensure grouping at (UMN, Execution Number, Date) level
      ),
      failure_data AS (
          -- Count only those where the final status = 'FAILURE'
          SELECT
              fs.created_date,
              handle,
              COUNT(DISTINCT combi) AS failure_count
          FROM final_status fs
          WHERE fs.final_status = 'FAILURE'  -- ✅ Only include UMNs where the final max status is FAILURE
          GROUP BY 1, 2
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'paytm' THEN failure_count ELSE NULL END) AS "paytm Failure",
          MAX(CASE WHEN handle = 'ptaxis' THEN failure_count ELSE NULL END) AS "ptaxis Failure",
          MAX(CASE WHEN handle = 'pthdfc' THEN failure_count ELSE NULL END) AS "pthdfc Failure",
          MAX(CASE WHEN handle = 'ptsbi' THEN failure_count ELSE NULL END) AS "ptsbi Failure",
          MAX(CASE WHEN handle = 'ptyes' THEN failure_count ELSE NULL END) AS "ptyes Failure",
          -- Total Failure Column
          (COALESCE(MAX(CASE WHEN handle = 'paytm' THEN failure_count ELSE NULL END), 0) +
           COALESCE(MAX(CASE WHEN handle = 'ptaxis' THEN failure_count ELSE NULL END), 0) +
           COALESCE(MAX(CASE WHEN handle = 'pthdfc' THEN failure_count ELSE NULL END), 0) +
           COALESCE(MAX(CASE WHEN handle = 'ptsbi' THEN failure_count ELSE NULL END), 0) +
           COALESCE(MAX(CASE WHEN handle = 'ptyes' THEN failure_count ELSE NULL END), 0)) AS "Total Failure"
      FROM failure_data
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

  dimension: total_failure {
    type: number
    label: "Total Failure"
    sql: ${TABLE}."Total Failure" ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm_failure,
      ptaxis_failure,
      pthdfc_failure,
      ptsbi_failure,
      ptyes_failure,
      total_failure
    ]
  }
}
