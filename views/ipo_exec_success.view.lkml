view: ipo_exec_success {
  derived_table: {
    sql: WITH revoke_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              SUBSTRING(umn FROM POSITION('@' IN umn) + 1) AS handle,
              COUNT(ti.umn) AS success_count
          FROM hive.switch.txn_info_snapshot_v3 ti
          JOIN hive.switch.txn_participants_snapshot_v3 tp
              ON ti.txn_id = tp.txn_id
          WHERE
              ti.type = 'COLLECT'
              AND ti.status = 'SUCCESS'  -- Only count successful revokes
              AND tp.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict $.purpose') = '"01"'
              AND tp.mcc = '6211'
              AND tp.participant_type = 'PAYEE'
          GROUP BY 1, 2
      )

      SELECT
      created_date,
      SUM(CASE WHEN handle = 'paytm' THEN success_count ELSE 0 END) AS paytm,
      SUM(CASE WHEN handle = 'ptaxis' THEN success_count ELSE 0 END) AS ptaxis,
      SUM(CASE WHEN handle = 'pthdfc' THEN success_count ELSE 0 END) AS pthdfc,
      SUM(CASE WHEN handle = 'ptsbi' THEN success_count ELSE 0 END) AS ptsbi,
      SUM(CASE WHEN handle = 'ptyes' THEN success_count ELSE 0 END) AS ptyes,
      SUM(success_count) AS total
      FROM revoke_data
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

  dimension: paytm {
    type: number
    sql: ${TABLE}.paytm ;;
  }

  dimension: ptaxis {
    type: number
    sql: ${TABLE}.ptaxis ;;
  }

  dimension: pthdfc {
    type: number
    sql: ${TABLE}.pthdfc ;;
  }

  dimension: ptsbi {
    type: number
    sql: ${TABLE}.ptsbi ;;
  }

  dimension: ptyes {
    type: number
    sql: ${TABLE}.ptyes ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  set: detail {
    fields: [
      created_date,
      paytm,
      ptaxis,
      pthdfc,
      ptsbi,
      ptyes,
      total
    ]
  }
}
