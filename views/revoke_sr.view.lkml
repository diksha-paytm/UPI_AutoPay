view: revoke_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) AS handle,
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'SUCCESS' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN ti.status = 'FAILURE' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated IS NOT NULL
              AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
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
              CONCAT(
                  CAST(ROUND(success * 100.0 / NULLIF(success + failure, 0), 2) AS VARCHAR),
                  '%'
              ) AS sr
          FROM
              handle_data
          WHERE
              handle IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes', 'paytm')
      )
      SELECT
          created_date,
          MAX(CASE WHEN handle = 'paytm' THEN sr ELSE NULL END) AS "paytm SR",
          MAX(CASE WHEN handle = 'ptaxis' THEN sr ELSE NULL END) AS "ptaxis SR",
          MAX(CASE WHEN handle = 'pthdfc' THEN sr ELSE NULL END) AS "pthdfc SR",
          MAX(CASE WHEN handle = 'ptsbi' THEN sr ELSE NULL END) AS "ptsbi SR",
          MAX(CASE WHEN handle = 'ptyes' THEN sr ELSE NULL END) AS "ptyes SR"
      FROM
          pivoted_data
      GROUP BY
          created_date
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

  set: detail {
    fields: [
      created_date,
      paytm_sr,
      ptaxis_sr,
      pthdfc_sr,
      ptsbi_sr,
      ptyes_sr
    ]
  }
}
