view: ptyes_vs_others_creations_error_contri {
  derived_table: {
    sql: WITH base_data AS (
          -- Fetch all relevant failure data for the last 7 days
          SELECT
              npci_resp_code,
              app_resp_code,
              DATE(created_on) AS txn_date,
              SUBSTRING(umn FROM POSITION('@' IN umn) + 1) AS handle,
              COUNT(DISTINCT umn) AS failures
          FROM hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated >= DATE_ADD('day', -7, CURRENT_DATE)
              AND created_on >= CAST(DATE_ADD('day', -7, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'CREATE'
              AND status = 'FAILURE'
              AND SUBSTRING(umn FROM POSITION('@' IN umn) + 1) IN ('ptsbi','ptyes', 'pthdfc', 'ptaxis')
          GROUP BY 1, 2, 3, 4
      ),
      total_data AS (
          -- Fetch total transactions per handle (required for failure percentage calculation)
          SELECT
              DATE(created_on) AS txn_date,
              SUBSTRING(umn FROM POSITION('@' IN umn) + 1) AS handle,
              COUNT(DISTINCT umn) AS total_txns
          FROM hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated >= DATE_ADD('day', -7, CURRENT_DATE)
              AND created_on >= CAST(DATE_ADD('day', -7, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'CREATE'
          GROUP BY 1, 2
      ),
      failure_rates AS (
          -- Join failure counts with total transactions to calculate failure percentage
          SELECT
              b.npci_resp_code,
              b.app_resp_code,
              b.txn_date,
              b.handle,
              b.failures,
              t.total_txns,
              CAST(ROUND((b.failures * 100.0) / NULLIF(t.total_txns, 0), 2) AS DECIMAL(10,1)) AS failure_rate
          FROM base_data b
          JOIN total_data t
              ON b.txn_date = t.txn_date
              AND b.handle = t.handle
      ),
      pivot_data AS (
          -- Pivot to get failure rates per handle in separate columns
          SELECT
              npci_resp_code,
              app_resp_code,
              txn_date,
              MAX(CASE WHEN handle = 'ptyes' THEN failure_rate ELSE NULL END) AS ptyes_failure_rate,
              MAX(CASE WHEN handle = 'pthdfc' THEN failure_rate ELSE NULL END) AS pthdfc_failure_rate,
              MAX(CASE WHEN handle = 'ptaxis' THEN failure_rate ELSE NULL END) AS ptaxis_failure_rate,
              MAX(CASE WHEN handle = 'ptsbi' THEN failure_rate ELSE NULL END) AS ptsbi_failure_rate
          FROM failure_rates
          GROUP BY 1, 2, 3
      ),
      last_7_days_agg AS (
          -- Aggregate failures and transactions over the last 7 days
          SELECT
              npci_resp_code,
              app_resp_code,
              SUM(CASE WHEN handle = 'ptyes' THEN failures ELSE 0 END) AS ptyes_failures,
              SUM(CASE WHEN handle = 'ptyes' THEN total_txns ELSE 0 END) AS ptyes_total_txns,
              SUM(CASE WHEN handle = 'pthdfc' THEN failures ELSE 0 END) AS pthdfc_failures,
              SUM(CASE WHEN handle = 'pthdfc' THEN total_txns ELSE 0 END) AS pthdfc_total_txns,
              SUM(CASE WHEN handle = 'ptaxis' THEN failures ELSE 0 END) AS ptaxis_failures,
              SUM(CASE WHEN handle = 'ptaxis' THEN total_txns ELSE 0 END) AS ptaxis_total_txns,
              SUM(CASE WHEN handle = 'ptsbi' THEN failures ELSE 0 END) AS ptsbi_failures,
              SUM(CASE WHEN handle = 'ptsbi' THEN total_txns ELSE 0 END) AS ptsbi_total_txns
          FROM failure_rates
          WHERE txn_date >= CAST(DATE_ADD('day', -7, CURRENT_DATE) AS TIMESTAMP)
          and txn_date < CAST(CURRENT_DATE AS TIMESTAMP)
          GROUP BY 1, 2
      )
      -- Final SELECT: Calculate differences for different timeframes
      SELECT
         p.npci_resp_code,
          p.app_resp_code ,

      -- D-1 Differences
      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(CURRENT_DATE AS TIMESTAMP)
      THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-1 ptyes vs pthdfc",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(CURRENT_DATE AS TIMESTAMP)
      THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-1 ptyes vs ptaxis",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(CURRENT_DATE AS TIMESTAMP)
      THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-1 ptyes vs ptsbi",

      -- D-2 Differences
      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-2 ptyes vs pthdfc",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-2 ptyes vs ptaxis",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-2 ptyes vs ptsbi",

      -- D-3 Differences
      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-3 ptyes vs pthdfc",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-3 ptyes vs ptaxis",

      CAST(MAX(CASE WHEN p.txn_date >= CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      and p.txn_date < CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END)
      AS VARCHAR) || '%' AS "D-3 ptyes vs ptsbi",

      -- Last 7 Days Differences (SUM of last 7 days)
      CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.pthdfc_failures * 100.0 / NULLIF(l.pthdfc_total_txns, 0)),
      2
      ) AS VARCHAR) || '%' AS "Last 7 Days ptyes vs pthdfc",

      CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.ptaxis_failures * 100.0 / NULLIF(l.ptaxis_total_txns, 0)),
      2
      ) AS VARCHAR) || '%' AS "Last 7 Days ptyes vs ptaxis",

      CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.ptsbi_failures * 100.0 / NULLIF(l.ptsbi_total_txns, 0)),
      2
      ) AS VARCHAR) || '%' AS "Last 7 Days ptyes vs ptsbi"

      FROM pivot_data p
      LEFT JOIN last_7_days_agg l
      ON p.npci_resp_code = l.npci_resp_code
      AND p.app_resp_code = l.app_resp_code
      GROUP BY 1, 2, l.ptyes_failures, l.ptyes_total_txns, l.pthdfc_failures, l.pthdfc_total_txns, l.ptaxis_failures, l.ptaxis_total_txns, l.ptsbi_failures, l.ptsbi_total_txns
      HAVING
      MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - pthdfc_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptaxis_failure_rate ELSE NULL END) IS NOT NULL
      OR MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP) THEN ptyes_failure_rate - ptsbi_failure_rate ELSE NULL END) IS NOT NULL
      OR CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.pthdfc_failures * 100.0 / NULLIF(l.pthdfc_total_txns, 0)),
      2
      ) AS DECIMAL(10,1)) IS NOT NULL
      OR CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.ptaxis_failures * 100.0 / NULLIF(l.ptaxis_total_txns, 0)),
      2
      ) AS DECIMAL(10,1)) IS NOT NULL
      OR CAST(ROUND(
      (l.ptyes_failures * 100.0 / NULLIF(l.ptyes_total_txns, 0)) -
      (l.ptsbi_failures * 100.0 / NULLIF(l.ptsbi_total_txns, 0)),
      2
      ) AS DECIMAL(10,1)) IS NOT NULL

      ORDER BY npci_resp_code, app_resp_code
      ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: app_resp_code {
    type: string
    sql: ${TABLE}.app_resp_code ;;
  }

  dimension: d1_ptyes_vs_pthdfc {
    type: string
    label: "D-1 ptyes vs pthdfc"
    sql: ${TABLE}."D-1 ptyes vs pthdfc" ;;
  }

  dimension: d1_ptyes_vs_ptaxis {
    type: string
    label: "D-1 ptyes vs ptaxis"
    sql: ${TABLE}."D-1 ptyes vs ptaxis" ;;
  }

  dimension: d2_ptyes_vs_pthdfc {
    type: string
    label: "D-2 ptyes vs pthdfc"
    sql: ${TABLE}."D-2 ptyes vs pthdfc" ;;
  }

  dimension: d2_ptyes_vs_ptaxis {
    type: string
    label: "D-2 ptyes vs ptaxis"
    sql: ${TABLE}."D-2 ptyes vs ptaxis" ;;
  }

  dimension: d3_ptyes_vs_pthdfc {
    type: string
    label: "D-3 ptyes vs pthdfc"
    sql: ${TABLE}."D-3 ptyes vs pthdfc" ;;
  }

  dimension: d3_ptyes_vs_ptaxis {
    type: string
    label: "D-3 ptyes vs ptaxis"
    sql: ${TABLE}."D-3 ptyes vs ptaxis" ;;
  }

  dimension: last_7_days_ptyes_vs_pthdfc {
    type: string
    label: "Last 7 Days ptyes vs pthdfc"
    sql: ${TABLE}."Last 7 Days ptyes vs pthdfc" ;;
  }

  dimension: last_7_days_ptyes_vs_ptaxis {
    type: string
    label: "Last 7 Days ptyes vs ptaxis"
    sql: ${TABLE}."Last 7 Days ptyes vs ptaxis" ;;
  }

  set: detail {
    fields: [
      npci_resp_code,
      app_resp_code,
      d1_ptyes_vs_pthdfc,
      d1_ptyes_vs_ptaxis,
      d2_ptyes_vs_pthdfc,
      d2_ptyes_vs_ptaxis,
      d3_ptyes_vs_pthdfc,
      d3_ptyes_vs_ptaxis,
      last_7_days_ptyes_vs_pthdfc,
      last_7_days_ptyes_vs_ptaxis
    ]
  }
}
