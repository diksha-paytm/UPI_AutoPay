view: creations_sr_across_mode {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              replace(
    json_query(ti.extended_info, 'strict $.initiationMode'),
    '"',
    ''
  ) initiation_mode,
              COUNT(distinct CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(distinct CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
              AND SUBSTRING(ti.umn, POSITION('@' IN ti.umn) + 1) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm','paytm')
          GROUP BY
              1,2
      ),
      pivoted_data AS (
          SELECT
              created_date,
              initiation_mode,
              CONCAT(
                  CAST(ROUND(success * 100.0 / NULLIF(success + failure, 0), 2) AS VARCHAR),
                  '%'
              ) AS sr
          FROM
              handle_data
          WHERE
              initiation_mode IN ('00','04','13')
      )
      SELECT
          created_date,
          MAX(CASE WHEN initiation_mode = '00' THEN sr ELSE NULL END) AS "Collect SR",
          MAX(CASE WHEN initiation_mode = '04' THEN sr ELSE NULL END) AS "Intent SR",
          MAX(CASE WHEN initiation_mode = '13' THEN sr ELSE NULL END) AS "QR SR"
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

  dimension: collect_sr {
    type: string
    label: "Collect SR"
    sql: ${TABLE}."Collect SR" ;;
  }

  dimension: intent_sr {
    type: string
    label: "Intent SR"
    sql: ${TABLE}."Intent SR" ;;
  }

  dimension: qr_sr {
    type: string
    label: "QR SR"
    sql: ${TABLE}."QR SR" ;;
  }

  set: detail {
    fields: [created_date, collect_sr, intent_sr, qr_sr]
  }
}
