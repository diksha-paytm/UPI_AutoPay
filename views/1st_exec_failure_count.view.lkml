view: 1st_exec_failure_count {
  derived_table: {
    sql: WITH final_status AS (
          -- Get final status per (UMN, Execution Number) combination for each date
          SELECT
              DATE(ti.created_on) AS created_date,
              CONCAT(
                  ti.umn,
                  REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
              ) AS unique_txn
          FROM team_product.looker_RM ti
          WHERE
              ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
          GROUP BY 1, 2
          HAVING MAX_BY(ti.status, ti.created_on) = 'FAILURE' -- Ensure final status for that date is FAILURE
      ),
      handle_data AS (
          -- Count only those failures where final status = 'FAILURE' (date-wise)
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              COUNT(DISTINCT fs.unique_txn) AS failure
          FROM team_product.looker_RM ti
          JOIN final_status fs
              ON CONCAT(
                  ti.umn,
                  REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
              ) = fs.unique_txn
              AND DATE(ti.created_on) = fs.created_date -- Ensure final status check is per date

      GROUP BY 1, 2
      ),
      pivoted_data AS (
      SELECT
      created_date,
      handle,
      failure
      FROM handle_data
      WHERE handle IN ('paytm','ptaxis', 'pthdfc', 'ptsbi', 'ptyes')
      )
      SELECT
      created_date,
      MAX(CASE WHEN handle = 'paytm' THEN failure ELSE NULL END) AS "paytm Failure",
      MAX(CASE WHEN handle = 'ptaxis' THEN failure ELSE NULL END) AS "ptaxis Failure",
      MAX(CASE WHEN handle = 'pthdfc' THEN failure ELSE NULL END) AS "pthdfc Failure",
      MAX(CASE WHEN handle = 'ptsbi' THEN failure ELSE NULL END) AS "ptsbi Failure",
      MAX(CASE WHEN handle = 'ptyes' THEN failure ELSE NULL END) AS "ptyes Failure",
      -- Total Failure Column
      (COALESCE(MAX(CASE WHEN handle = 'paytm' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptaxis' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'pthdfc' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptsbi' THEN failure ELSE NULL END), 0) +
      COALESCE(MAX(CASE WHEN handle = 'ptyes' THEN failure ELSE NULL END), 0)) AS "Total Failure"
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
