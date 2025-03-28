view: ptyes_vs_others_creations_sr {
  derived_table: {
    sql: WITH base_sr AS (
          -- Compute distinct transaction counts per handle per day
          SELECT
              DATE(created_on) AS txn_date,
              SUBSTRING(umn FROM POSITION('@' IN umn) + 1) AS handle,
              COUNT(DISTINCT umn) AS total_txns,
              COUNT(DISTINCT CASE WHEN status = 'SUCCESS' THEN umn ELSE NULL END) AS success_txns
          FROM hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated >= DATE_ADD('day', -7, CURRENT_DATE)
              AND created_on >= CAST(DATE_ADD('day', -7, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'CREATE'
              AND SUBSTRING(umn FROM POSITION('@' IN umn) + 1) IN ('ptsbi', 'ptyes', 'ptaxis', 'pthdfc')
          GROUP BY 1, 2
      ),
      pivot_data AS (
          -- Pivot to get success rates per handle in separate columns
          SELECT
              txn_date,
              ROUND(MAX(CASE WHEN handle = 'ptyes' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END), 2) AS ptyes_sr,
              ROUND(MAX(CASE WHEN handle = 'pthdfc' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END), 2) AS pthdfc_sr,
              ROUND(MAX(CASE WHEN handle = 'ptaxis' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END), 2) AS ptaxis_sr,
              ROUND(MAX(CASE WHEN handle = 'ptsbi' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END), 2) AS ptsbi_sr
          FROM base_sr
          GROUP BY 1
      ),
      last_7_days_agg AS (
          -- Aggregate success rates over the last 7 days
          SELECT
              ROUND(AVG(p.ptyes_sr), 2) AS last_7_ptyes_sr,
              ROUND(AVG(p.pthdfc_sr), 2) AS last_7_pthdfc_sr,
              ROUND(AVG(p.ptaxis_sr), 2) AS last_7_ptaxis_sr,
              ROUND(AVG(p.ptsbi_sr), 2) AS last_7_ptsbi_sr
          FROM pivot_data p
      )
      SELECT
          'SR' AS Metric,
          '%' AS Unit,
          -- Success Rates for D-1
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -1, CURRENT_DATE) THEN p.ptaxis_sr ELSE NULL END) AS VARCHAR), '%') AS "D-1 ptaxis",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -1, CURRENT_DATE) THEN p.pthdfc_sr ELSE NULL END) AS VARCHAR), '%') AS "D-1 pthdfc",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -1, CURRENT_DATE) THEN p.ptsbi_sr ELSE NULL END) AS VARCHAR), '%') AS "D-1 ptsbi",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -1, CURRENT_DATE) THEN p.ptyes_sr ELSE NULL END) AS VARCHAR), '%') AS "D-1 ptyes",
          -- Success Rates for D-2
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -2, CURRENT_DATE) THEN p.ptaxis_sr ELSE NULL END) AS VARCHAR), '%') AS "D-2 ptaxis",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -2, CURRENT_DATE) THEN p.pthdfc_sr ELSE NULL END) AS VARCHAR), '%') AS "D-2 pthdfc",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -2, CURRENT_DATE) THEN p.ptsbi_sr ELSE NULL END) AS VARCHAR), '%') AS "D-2 ptsbi",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -2, CURRENT_DATE) THEN p.ptyes_sr ELSE NULL END) AS VARCHAR), '%') AS "D-2 ptyes",
          -- Success Rates for D-3
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -3, CURRENT_DATE) THEN p.ptaxis_sr ELSE NULL END) AS VARCHAR), '%') AS "D-3 ptaxis",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -3, CURRENT_DATE) THEN p.pthdfc_sr ELSE NULL END) AS VARCHAR), '%') AS "D-3 pthdfc",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -3, CURRENT_DATE) THEN p.ptsbi_sr ELSE NULL END) AS VARCHAR), '%') AS "D-3 ptsbi",
          CONCAT(CAST(MAX(CASE WHEN p.txn_date = DATE_ADD('day', -3, CURRENT_DATE) THEN p.ptyes_sr ELSE NULL END) AS VARCHAR), '%') AS "D-3 ptyes",
          -- Last 7 Days Success Rates
          CONCAT(CAST(l.last_7_ptaxis_sr AS VARCHAR), '%') AS "Last 7 Days ptaxis",
          CONCAT(CAST(l.last_7_pthdfc_sr AS VARCHAR), '%') AS "Last 7 Days pthdfc",
          CONCAT(CAST(l.last_7_ptsbi_sr AS VARCHAR), '%') AS "Last 7 Days ptsbi",
          CONCAT(CAST(l.last_7_ptyes_sr AS VARCHAR), '%') AS "Last 7 Days ptyes"
      FROM pivot_data p
      LEFT JOIN last_7_days_agg l ON 1=1
      GROUP BY l.last_7_ptyes_sr, l.last_7_pthdfc_sr, l.last_7_ptaxis_sr, l.last_7_ptsbi_sr
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: metric {
    type: string
    sql: ${TABLE}.Metric ;;
  }

  dimension: unit {
    type: string
    sql: ${TABLE}.Unit ;;
  }

  dimension: d1_ptaxis {
    type: string
    label: "D-1 ptaxis"
    sql: ${TABLE}."D-1 ptaxis" ;;
  }

  dimension: d1_pthdfc {
    type: string
    label: "D-1 pthdfc"
    sql: ${TABLE}."D-1 pthdfc" ;;
  }

  dimension: d1_ptsbi {
    type: string
    label: "D-1 ptsbi"
    sql: ${TABLE}."D-1 ptsbi" ;;
  }

  dimension: d1_ptyes {
    type: string
    label: "D-1 ptyes"
    sql: ${TABLE}."D-1 ptyes" ;;
  }

  dimension: d2_ptaxis {
    type: string
    label: "D-2 ptaxis"
    sql: ${TABLE}."D-2 ptaxis" ;;
  }

  dimension: d2_pthdfc {
    type: string
    label: "D-2 pthdfc"
    sql: ${TABLE}."D-2 pthdfc" ;;
  }

  dimension: d2_ptsbi {
    type: string
    label: "D-2 ptsbi"
    sql: ${TABLE}."D-2 ptsbi" ;;
  }

  dimension: d2_ptyes {
    type: string
    label: "D-2 ptyes"
    sql: ${TABLE}."D-2 ptyes" ;;
  }

  dimension: d3_ptaxis {
    type: string
    label: "D-3 ptaxis"
    sql: ${TABLE}."D-3 ptaxis" ;;
  }

  dimension: d3_pthdfc {
    type: string
    label: "D-3 pthdfc"
    sql: ${TABLE}."D-3 pthdfc" ;;
  }

  dimension: d3_ptsbi {
    type: string
    label: "D-3 ptsbi"
    sql: ${TABLE}."D-3 ptsbi" ;;
  }

  dimension: d3_ptyes {
    type: string
    label: "D-3 ptyes"
    sql: ${TABLE}."D-3 ptyes" ;;
  }

  dimension: last_7_days_ptaxis {
    type: string
    label: "Last 7 Days ptaxis"
    sql: ${TABLE}."Last 7 Days ptaxis" ;;
  }

  dimension: last_7_days_pthdfc {
    type: string
    label: "Last 7 Days pthdfc"
    sql: ${TABLE}."Last 7 Days pthdfc" ;;
  }

  dimension: last_7_days_ptsbi {
    type: string
    label: "Last 7 Days ptsbi"
    sql: ${TABLE}."Last 7 Days ptsbi" ;;
  }

  dimension: last_7_days_ptyes {
    type: string
    label: "Last 7 Days ptyes"
    sql: ${TABLE}."Last 7 Days ptyes" ;;
  }

  set: detail {
    fields: [
      metric,
      unit,
      d1_ptaxis,
      d1_pthdfc,
      d1_ptsbi,
      d1_ptyes,
      d2_ptaxis,
      d2_pthdfc,
      d2_ptsbi,
      d2_ptyes,
      d3_ptaxis,
      d3_pthdfc,
      d3_ptsbi,
      d3_ptyes,
      last_7_days_ptaxis,
      last_7_days_pthdfc,
      last_7_days_ptsbi,
      last_7_days_ptyes
    ]
  }
}
