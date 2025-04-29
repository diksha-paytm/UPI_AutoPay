view: cc_1st_exec_payee_wise_success {
  derived_table: {
    sql: WITH all_payees AS (
          -- Get all distinct payee VPAs and names from successful transactions
          SELECT DISTINCT
              tp1.vpa AS payee_vpa,
              tp1.name AS payee_name
          FROM team_product.looker_RM_CC ti
          JOIN team_product.looker_txn_parti_RM tp1
            ON ti.txn_id = tp1.txn_id
          WHERE
              ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND ti.status = 'SUCCESS'
              AND tp1.participant_type = 'PAYEE'

      ),

      all_dates_successful_txns AS (
      -- Fetch successful transactions for all dates but only for these payees
      SELECT
      DATE(ti.created_on) AS created_date,
      tp1.vpa AS payee_vpa,
      tp1.name AS payee_name,
      COUNT(DISTINCT ti.umn) AS count
      FROM team_product.looker_RM_CC ti
      JOIN team_product.looker_txn_parti_RM tp1
      ON ti.txn_id = tp1.txn_id
      JOIN all_payees ap
      ON tp1.vpa = ap.payee_vpa AND tp1.name = ap.payee_name
      WHERE
      ti.type = 'COLLECT'
      AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
      AND ti.status = 'SUCCESS'
      AND tp1.participant_type = 'PAYEE'
      GROUP BY 1,2,3
      )

      -- Final Output
      SELECT *
      FROM all_dates_successful_txns
      ORDER BY created_date DESC, count DESC
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
