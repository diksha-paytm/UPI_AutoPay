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
              AND SUBSTRING(umn FROM POSITION('@' IN umn) + 1) IN ('ptsbi','ptyes', 'ptaxis', 'pthdfc')
          GROUP BY 1, 2
      ),
      pivot_data AS (
          -- Pivot to get success rates per handle in separate columns
          SELECT
              txn_date,
              ROUND(
                  MAX(CASE WHEN handle = 'ptyes' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END),
                  2
              ) AS ptyes_sr,
              ROUND(
                  MAX(CASE WHEN handle = 'pthdfc' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END),
                  2
              ) AS pthdfc_sr,
              ROUND(
                  MAX(CASE WHEN handle = 'ptaxis' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END),
                  2
              ) AS ptaxis_sr,
              ROUND(
                  MAX(CASE WHEN handle = 'ptsbi' THEN success_txns * 100.0 / NULLIF(total_txns, 0) ELSE NULL END),
                  2
              ) AS ptsbi_sr
          FROM base_sr
          GROUP BY 1
      ),
      last_7_days_agg AS (
          -- Aggregate success rates over the last 7 days
          SELECT
              ROUND(SUM(p.ptyes_sr) / COUNT(p.ptyes_sr), 2) AS last_7_ptyes_sr,
              ROUND(SUM(p.pthdfc_sr) / COUNT(p.pthdfc_sr), 2) AS last_7_pthdfc_sr,
              ROUND(SUM(p.ptaxis_sr) / COUNT(p.ptaxis_sr), 2) AS last_7_ptaxis_sr,
              ROUND(SUM(p.ptsbi_sr) / COUNT(p.ptsbi_sr), 2) AS last_7_ptsbi_sr
          FROM pivot_data p
      )
      SELECT
          'SR' AS SR,
          '%' AS percentage,

      -- D-1 Success Rates
      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptaxis_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-1 (ptyes - ptaxis)",

      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.pthdfc_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-1 (ptyes - pthdfc)",

      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -1, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptsbi_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-1 (ptyes - ptsbi)",

      -- D-2 Success Rates
      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptaxis_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-2 (ptyes - ptaxis)",

      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.pthdfc_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-2 (ptyes - pthdfc)",

      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -2, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptsbi_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-2 (ptyes - ptsbi)",

      -- D-3 Success Rates
      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptaxis_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-3 (ptyes - ptaxis)",

      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.pthdfc_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-3 (ptyes - pthdfc)",
      CONCAT(
      CAST(MAX(CASE WHEN p.txn_date = CAST(DATE_ADD('day', -3, CURRENT_DATE) AS TIMESTAMP)
      THEN p.ptyes_sr - p.ptsbi_sr ELSE NULL END) AS VARCHAR),
      '%'
      ) AS "D-3 (ptyes - ptsbi)",

      -- Last 7 Days Success Rates
      CONCAT(
      CAST((l.last_7_ptyes_sr - l.last_7_ptaxis_sr) AS VARCHAR),
      '%'
      ) AS "Last 7 Days (ptyes - ptaxis)",

      CONCAT(
      CAST((l.last_7_ptyes_sr - l.last_7_pthdfc_sr) AS VARCHAR),
      '%'
      ) AS "Last 7 Days (ptyes - pthdfc)",

      CONCAT(
      CAST((l.last_7_ptyes_sr - l.last_7_ptsbi_sr) AS VARCHAR),
      '%'
      ) AS "Last 7 Days (ptyes - ptsbi)"

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

  dimension: sr {
    type: string
    sql: ${TABLE}.SR ;;
  }

  dimension: percentage {
    type: string
    sql: ${TABLE}.percentage ;;
  }

  dimension: d1_ptyes__ptaxis {
    type: string
    label: "D-1 (ptyes - ptaxis)"
    sql: ${TABLE}."D-1 (ptyes - ptaxis)" ;;
  }

  dimension: d1_ptyes__pthdfc {
    type: string
    label: "D-1 (ptyes - pthdfc)"
    sql: ${TABLE}."D-1 (ptyes - pthdfc)" ;;
  }

  dimension: d2_ptyes__ptaxis {
    type: string
    label: "D-2 (ptyes - ptaxis)"
    sql: ${TABLE}."D-2 (ptyes - ptaxis)" ;;
  }

  dimension: d2_ptyes__pthdfc {
    type: string
    label: "D-2 (ptyes - pthdfc)"
    sql: ${TABLE}."D-2 (ptyes - pthdfc)" ;;
  }

  dimension: d3_ptyes__ptaxis {
    type: string
    label: "D-3 (ptyes - ptaxis)"
    sql: ${TABLE}."D-3 (ptyes - ptaxis)" ;;
  }

  dimension: d3_ptyes__pthdfc {
    type: string
    label: "D-3 (ptyes - pthdfc)"
    sql: ${TABLE}."D-3 (ptyes - pthdfc)" ;;
  }

  dimension: last_7_days_ptyes__ptaxis {
    type: string
    label: "Last 7 Days (ptyes - ptaxis)"
    sql: ${TABLE}."Last 7 Days (ptyes - ptaxis)" ;;
  }

  dimension: last_7_days_ptyes__pthdfc {
    type: string
    label: "Last 7 Days (ptyes - pthdfc)"
    sql: ${TABLE}."Last 7 Days (ptyes - pthdfc)" ;;
  }

  set: detail {
    fields: [
      sr,
      percentage,
      d1_ptyes__ptaxis,
      d1_ptyes__pthdfc,
      d2_ptyes__ptaxis,
      d2_ptyes__pthdfc,
      d3_ptyes__ptaxis,
      d3_ptyes__pthdfc,
      last_7_days_ptyes__ptaxis,
      last_7_days_ptyes__pthdfc
    ]
  }
}
