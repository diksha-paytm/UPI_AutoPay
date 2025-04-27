view: 1st_exec_user_sr {
  derived_table: {
    sql: WITH base_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              CONCAT(
                  tp.scope_cust_id,
                  REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
              ) AS txn_key,
              MAX_BY(ti.status, ti.created_on) AS final_status
          FROM
              team_product.looker_RM ti
              join team_product.looker_txn_parti_RM tp
              on ti.txn_id= tp.txn_id
          WHERE
              ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
          GROUP BY
              1, 2, 3
      ),

      handle_data AS (
      SELECT
      created_date,
      handle,
      ROUND(
      COUNT(DISTINCT CASE WHEN final_status = 'SUCCESS' THEN txn_key END) * 100.0 /
      COUNT(DISTINCT txn_key),
      2
      ) AS sr
      FROM base_data
      GROUP BY 1, 2
      ),

      aggregated_data AS (
      SELECT
      created_date,
      MAX(CASE WHEN handle = 'ptaxis' THEN sr END) AS ptaxis_sr,
      MAX(CASE WHEN handle = 'pthdfc' THEN sr END) AS pthdfc_sr,
      MAX(CASE WHEN handle = 'ptsbi' THEN sr END) AS ptsbi_sr,
      MAX(CASE WHEN handle = 'ptyes' THEN sr END) AS ptyes_sr
      FROM handle_data
      GROUP BY created_date
      )

      SELECT
      created_date,
      CONCAT(CAST(ptaxis_sr AS VARCHAR), '%') AS "ptaxis SR",
      CONCAT(CAST(pthdfc_sr AS VARCHAR), '%') AS "pthdfc SR",
      CONCAT(CAST(ptsbi_sr AS VARCHAR), '%') AS "ptsbi SR",
      CONCAT(CAST(ptyes_sr AS VARCHAR), '%') AS "ptyes SR",
      CONCAT(
      CAST(
      ROUND(
      (COALESCE(ptaxis_sr, 0) +
      COALESCE(pthdfc_sr, 0) +
      COALESCE(ptsbi_sr, 0) +
      COALESCE(ptyes_sr, 0)) /
      NULLIF(
      (CASE WHEN ptaxis_sr IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN pthdfc_sr IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN ptsbi_sr IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN ptyes_sr IS NOT NULL THEN 1 ELSE 0 END),
      0
      ),
      2
      ) AS VARCHAR
      ),
      '%'
      ) AS "Average SR"
      FROM
      aggregated_data
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

  dimension: ptaxis_sr {
    type: string
    label: "ptaxis SR"
    sql: ${TABLE}."ptaxis SR" ;;
  }

  dimension: pthdfc_sr {
    type: string
    label: "pthdfc SR"
    sql: ${TABLE}."pthdfc SR" ;;
  }

  dimension: ptsbi_sr {
    type: string
    label: "ptsbi SR"
    sql: ${TABLE}."ptsbi SR" ;;
  }

  dimension: ptyes_sr {
    type: string
    label: "ptyes SR"
    sql: ${TABLE}."ptyes SR" ;;
  }

  dimension: average_sr {
    type: string
    label: "Average SR"
    sql: ${TABLE}."Average SR" ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_sr,
      pthdfc_sr,
      ptsbi_sr,
      ptyes_sr,
      average_sr
    ]
  }
}
