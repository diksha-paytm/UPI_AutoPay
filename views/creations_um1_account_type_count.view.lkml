view: creations_um1_account_type_count {
  derived_table: {
    sql: SELECT
          DATE(ti.created_on) AS created_date,
          tp.account_type,
          COUNT(DISTINCT ti.umn) AS failure,
          SUM(COUNT(DISTINCT ti.umn)) OVER (PARTITION BY DATE(ti.created_on)) AS total_failure
      FROM hive.switch.txn_info_snapshot_v3 ti
      JOIN hive.switch.txn_participants_snapshot_v3 tp
          ON ti.txn_id = tp.txn_id
      WHERE
          business_type = 'MANDATE'
          AND JSON_QUERY(ti.extended_info, 'strict $.purpose') = '"14"'
          AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
          AND tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
          AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
          AND ti.type = 'CREATE'
          AND ti.status = 'FAILURE'
          AND ti.npci_resp_code = 'UM1'
      GROUP BY DATE(ti.created_on), tp.account_type
      ORDER BY DATE(ti.created_on) DESC, failure DESC
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

  dimension: account_type {
    type: string
    sql: ${TABLE}.account_type ;;
  }

  dimension: failure {
    type: number
    sql: ${TABLE}.failure ;;
  }

  dimension: total_failure {
    type: number
    sql: ${TABLE}.total_failure ;;
  }

  set: detail {
    fields: [created_date, account_type, failure, total_failure]
  }
}
