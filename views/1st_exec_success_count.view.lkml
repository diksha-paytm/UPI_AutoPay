view: 1st_exec_success_count {
  derived_table: {
    sql: WITH final_status AS (
    -- Get final status per (UMN, Execution Number) combination for each date
    SELECT
        DATE(ti.created_on) AS created_date,
        CONCAT(
            ti.umn,
            REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
        ) AS unique_txn
    FROM hive.switch.txn_info_snapshot_v3 ti
    WHERE
        ti.business_type = 'MANDATE'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
        AND ti.type = 'COLLECT'
        AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
    GROUP BY 1, 2
    HAVING MAX_BY(ti.status, ti.created_on) = 'SUCCESS' -- Ensure final status for that date is FAILURE
),
handle_data AS (
    -- Count only those failures where final status = 'FAILURE' (date-wise)
    SELECT
        DATE(ti.created_on) AS created_date,
        SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
        COUNT(DISTINCT fs.unique_txn) AS failure
    FROM hive.switch.txn_info_snapshot_v3 ti
    JOIN final_status fs
        ON CONCAT(
            ti.umn,
            REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
        ) = fs.unique_txn
        AND DATE(ti.created_on) = fs.created_date -- Ensure final status check is per date
    WHERE
         ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY 1, 2
),
pivoted_data AS (
    SELECT
        created_date,
        handle,
        failure
    FROM handle_data
    WHERE handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
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
      ptaxis_success,
      pthdfc_success,
      ptsbi_success,
      ptyes_success,
      total_success
    ]
  }
}
