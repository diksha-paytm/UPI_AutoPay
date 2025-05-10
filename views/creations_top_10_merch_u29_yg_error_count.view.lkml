view: creations_top_10_merch_u29_yg_error_count {
  derived_table: {
    sql: WITH paytm_failures AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              tp.vpa AS payee_vpa,
              tp.name AS payee_name,
              COUNT(DISTINCT ti.umn) AS failure
          FROM hive.team_product.looker_RM ti
          JOIN team_product.looker_txn_parti_RM tp
              ON ti.txn_id = tp.txn_id
          WHERE
              ti.type = 'CREATE'
              AND tp.participant_type = 'PAYEE'
              AND ti.npci_resp_code = 'U29-YG'
              AND ti.status = 'FAILURE'
          GROUP BY 1, 2, 3
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
      count,
      row_num
      FROM filtered_vpa_failures

      UNION ALL

      SELECT
      dtf.created_date,
      'TOTAL' AS payee_vpa,
      'COUNT' AS payee_name,
      dtf.total_failures AS count,
      (SELECT MAX(row_num) + 1 FROM filtered_vpa_failures WHERE created_date = dtf.created_date) AS row_num
      FROM daily_total_failures dtf
      )

      SELECT
      created_date,
      payee_vpa,
      payee_name,
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

  dimension: count_ {
    type: number
    sql: ${TABLE}."count" ;;
  }

  set: detail {
    fields: [created_date, payee_vpa, payee_name, count_]
  }
}
