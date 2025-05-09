view: creations_top_10_merch_u16_error_count {
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
              AND ti.npci_resp_code = 'U16'
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
          SELECT
              pf.payee_vpa
          FROM paytm_failures pf
          JOIN latest_failures lf
              ON pf.created_date = lf.created_date
          ORDER BY pf.failure DESC
          LIMIT 10
      ),
      daily_total_failures AS (
          SELECT created_date, SUM(failure) AS total_failures
          FROM paytm_failures
          GROUP BY created_date
      )
      SELECT
          pf.created_date,
          pf.payee_vpa,
          MAX(pf.payee_name) AS payee_name,
          pf.failure AS count,
          dtf.total_failures AS total
      FROM paytm_failures pf
      JOIN top_10_vpa t5
          ON pf.payee_vpa = t5.payee_vpa
      JOIN daily_total_failures dtf
          ON pf.created_date = dtf.created_date
      GROUP BY pf.created_date, pf.payee_vpa, pf.failure, dtf.total_failures
      ORDER BY pf.created_date DESC, pf.failure DESC
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

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  set: detail {
    fields: [created_date, payee_vpa, payee_name, count_, total]
  }
}
