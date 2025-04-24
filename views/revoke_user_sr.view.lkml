view: revoke_user_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
            ROUND(
    COUNT(DISTINCT CASE
        WHEN status = 'SUCCESS'
        THEN tp.scope_cust_id
        ELSE NULL
      END
    ) * 100.0 /
  NULLIF(COUNT(DISTINCT tp.scope_cust_id), 0), 2
  ) AS sr
  FROM
             team_product.looker_RM ti
             join
             team_product.looker_txn_parti_RM tp
             on ti.txn_id = tp.txn_id
          WHERE
             ti.type = 'REVOKE'

      GROUP BY
      DATE(ti.created_on),
      SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      )
      SELECT
      created_date,
      -- Convert numeric SR to string with '%'
      CONCAT(CAST(MAX(CASE WHEN handle = 'paytm' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "paytm SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptaxis' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptaxis SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'pthdfc' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "pthdfc SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptsbi' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptsbi SR",
      CONCAT(CAST(MAX(CASE WHEN handle = 'ptyes' THEN sr ELSE NULL END) AS VARCHAR), '%') AS "ptyes SR",
      -- Calculate average before converting to string
      CONCAT(CAST(ROUND(AVG(sr), 2) AS VARCHAR), '%') AS "Average SR"
      FROM handle_data
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

  dimension: paytm_sr {
    type: string
    label: "paytm SR"
    sql: ${TABLE}."paytm SR" ;;
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
      paytm_sr,
      ptaxis_sr,
      pthdfc_sr,
      ptsbi_sr,
      ptyes_sr,
      average_sr
    ]
  }
}
