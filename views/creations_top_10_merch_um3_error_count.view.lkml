view: creations_top_10_merch_um3_error_count {
  derived_table: {
    sql: WITH paytm_failures AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              tp.vpa AS payee_vpa,
              tp.name AS payee_name,
      REGEXP_EXTRACT(CAST(ti.expire_on - ti.created_on AS VARCHAR), '[0-9]{2}:[0-9]{2}:[0-9]{2}') AS expiry_time,  -- Added expiry time calculation
              COUNT(DISTINCT ti.umn) AS failure
          FROM hive.switch.txn_info_snapshot_v3 ti
          JOIN hive.switch.txn_participants_snapshot_v3 tp
              ON ti.txn_id = tp.txn_id
          WHERE
        ti.business_type = 'MANDATE'
        and json_query(ti.extended_info, 'strict$.purpose') = '"14"'
        and ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
             and tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
      and tp.participant_type='PAYEE'
        and ti.type = 'CREATE'
        and npci_resp_code ='UM3'
      GROUP BY 1, 2, 3, 4
      ),

      latest_failures AS (
      SELECT created_date
      FROM paytm_failures
      ORDER BY created_date DESC
      LIMIT 1
      ),

      top_10_vpa AS (
      -- Find top 10 VPA for the latest day with their respective failure count
      SELECT
      pf.payee_vpa,
      MAX(pf.failure) AS max_failure
      FROM paytm_failures pf
      JOIN latest_failures lf
      ON pf.created_date = lf.created_date
      GROUP BY pf.payee_vpa
      ORDER BY max_failure DESC
      LIMIT 10
      ),

      daily_total_failures AS (
      SELECT
      created_date,
      SUM(failure) AS total_failures
      FROM paytm_failures
      GROUP BY created_date
      ),

      filtered_vpa_failures AS (
      -- Filter only the top 10 VPA for every date, avoiding duplicates
      SELECT DISTINCT
      pf.created_date,
      pf.payee_vpa,
      pf.payee_name,
      pf.expiry_time,
      pf.failure AS count,
      ROW_NUMBER() OVER (PARTITION BY pf.created_date ORDER BY pf.failure DESC) AS row_num
      FROM paytm_failures pf
      JOIN top_10_vpa t5
      ON pf.payee_vpa = t5.payee_vpa
      ),

      -- Add 'TOTAL' row for each date with proper row ordering
      final_data AS (
      SELECT
      created_date,
      payee_vpa,
      payee_name,
      expiry_time,
      count,
      row_num
      FROM filtered_vpa_failures

      UNION ALL

      SELECT
      dtf.created_date,
      'TOTAL' AS payee_vpa,
      'COUNT' AS payee_name,
      NULL AS expiry_time, -- Since 'TOTAL' is aggregate, expiry time doesn't apply
      dtf.total_failures AS count,
      (SELECT MAX(row_num) + 1 FROM filtered_vpa_failures WHERE created_date = dtf.created_date) AS row_num
      FROM daily_total_failures dtf
      )

      SELECT
      created_date,
      payee_vpa,
      payee_name,
      expiry_time,
      count
      FROM final_data
      ORDER BY created_date DESC, row_num
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

  dimension: payee_vpa {
    type: string
    sql: ${TABLE}.payee_vpa ;;
  }

  dimension: payee_name {
    type: string
    sql: ${TABLE}.payee_name ;;
  }

  dimension: expiry_time {
    type: string
    sql: ${TABLE}.expiry_time ;;
  }

  dimension: count_ {
    type: number
    sql: ${TABLE}."count" ;;
  }

  set: detail {
    fields: [created_date, payee_vpa, payee_name, expiry_time, count_]
  }
}
