view: revoke_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.txn_id END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.txn_id END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'REVOKE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              DATE(ti.created_on),
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              ROUND(success * 100.0 / NULLIF(success + failure, 0), 2) AS sr -- Keep SR as numeric
          FROM
              handle_data
          WHERE
              handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
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
