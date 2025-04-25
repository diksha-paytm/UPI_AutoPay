view: creations_handle_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
          --    round(
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'SUCCESS' THEN
                          ti.umn

      ELSE NULL
      END
      ) as success ,
      COUNT(
      DISTINCt
      ti.umn

      ) as overall

      FROM team_product.looker_RM ti
      WHERE
      ti.type = 'CREATE'
      -- AND ti.status IN ('FAILURE', 'SUCCESS')
      GROUP BY 1, 2
      ),
      pivoted_data AS (
      SELECT
      created_date,
      handle,
      ROUND(success * 100.0 / NULLIF(overall, 0), 2) AS sr -- Numeric type for aggregation
      FROM handle_data
      WHERE handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes')

      )SELECT
      created_date,
      -- Convert to VARCHAR with '%' in final select
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptaxis' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptaxis SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'pthdfc' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "pthdfc SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptsbi' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptsbi SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptyes' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptyes SR",
      -- Calculate Average SR and then format as percentage
      CONCAT(CAST(ROUND(AVG(sr), 2) AS VARCHAR), '%') AS "Average SR"
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
