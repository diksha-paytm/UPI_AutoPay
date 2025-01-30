view: rm_revoke_dtu {
  derived_table: {
    sql: SELECT
        DATE(ti.created_on) AS Date,
        COUNT(DISTINCT tp.scope_cust_id) AS users
      FROM
        hive.switch.txn_info_snapshot_v3 ti
        JOIN
        hive.switch.txn_participants_snapshot_v3 tp
        ON ti.txn_id = tp.txn_id
      WHERE
        ti.business_type = 'MANDATE'
        and json_query(ti.extended_info, 'strict$.purpose') = '"14"'
        and ti.type = 'REVOKE'
        AND ti.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -100,CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -100,CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
      GROUP BY
        1
      ORDER BY
        1 DESC -- Order by month in descending order
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    sql: ${TABLE}."Date" ;;
  }

  dimension: users {
    type: number
    sql: ${TABLE}.users ;;
  }

  set: detail {
    fields: [date, users]
  }
}
