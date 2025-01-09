view: creations_count {
  derived_table: {
    sql: SELECT
        DATE(ti.created_on) AS created_date,
        SUBSTRING(
          ti.umn
          FROM
            POSITION('@' IN ti.umn) + 1
        ) AS handle,
        REPLACE(
          JSON_QUERY(ti.extended_info, 'strict $.initiationMode'),
          '"',
          ''
        ) AS initiation_mode,
        SUM(CASE WHEN ti.status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN ti.status = 'FAILURE' THEN 1 ELSE 0 END) AS failure_count
      FROM
        hive.switch.txn_info_snapshot_v3 ti
      WHERE
        ti.business_type = 'MANDATE'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
       AND SUBSTRING(
          ti.umn
          FROM
            POSITION('@' IN ti.umn) + 1
        ) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm')
        AND dl_last_updated IS NOT NULL
        AND created_on >= CAST(DATE_ADD('day', -100,CURRENT_DATE) AS TIMESTAMP) -- Start 100 days before today
        AND created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
        AND ti.type = 'CREATE'
        AND ti.status IN ('FAILURE', 'SUCCESS')
      GROUP BY
        1, 2, 3
      ORDER BY
        1 desc, 2, 3
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

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: initiation_mode {
    type: string
    sql: ${TABLE}.initiation_mode ;;
  }

  dimension: success_count {
    type: number
    sql: ${TABLE}.success_count ;;
  }

  dimension: failure_count {
    type: number
    sql: ${TABLE}.failure_count ;;
  }

  set: detail {
    fields: [created_date, handle, initiation_mode, success_count, failure_count]
  }
}
