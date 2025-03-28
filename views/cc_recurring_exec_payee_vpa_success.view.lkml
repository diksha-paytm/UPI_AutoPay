view: cc_recurring_exec_payee_vpa_success {
  derived_table: {
    sql: WITH all_payees AS (
          -- Get all distinct payee VPAs and names from successful transactions
          SELECT DISTINCT
              tp1.vpa AS payee_vpa,
              tp1.name AS payee_name
          FROM hive.switch.txn_info_snapshot_v3 ti
          JOIN hive.switch.txn_participants_snapshot_v3 tp
           ON ti.txn_id = tp.txn_id
          JOIN hive.switch.txn_participants_snapshot_v3 tp1
            ON ti.txn_id = tp1.txn_id
          WHERE
              ti.business_type = 'MANDATE'
              AND tp.account_type = 'CREDIT'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND tp1.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
              AND ti.status = 'SUCCESS'
              AND tp1.participant_type = 'PAYEE'
              AND tp.participant_type = 'PAYER'

      ),

      all_dates_successful_txns AS (
      -- Fetch successful transactions for all dates but only for these payees
      SELECT
      DATE(ti.created_on) AS created_date,
      tp1.vpa AS payee_vpa,
      tp1.name AS payee_name,
      COUNT(DISTINCT ti.umn) AS successful_mandates
      FROM hive.switch.txn_info_snapshot_v3 ti
      JOIN hive.switch.txn_participants_snapshot_v3 tp
      ON ti.txn_id = tp.txn_id
      JOIN hive.switch.txn_participants_snapshot_v3 tp1
      ON ti.txn_id = tp1.txn_id
      JOIN all_payees ap
      ON tp1.vpa = ap.payee_vpa AND tp1.name = ap.payee_name
      WHERE
      ti.business_type = 'MANDATE'
      AND tp.account_type = 'CREDIT'
      AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
      AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND tp1.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
      AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
      AND ti.type = 'COLLECT'
      AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
      AND ti.status = 'SUCCESS'
      AND tp1.participant_type = 'PAYEE'
      AND tp.participant_type = 'PAYER'
      GROUP BY 1,2,3
      )

      -- Final Output
      SELECT *
      FROM all_dates_successful_txns
      ORDER BY created_date DESC, successful_mandates DESC
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

  dimension: successful_mandates {
    type: number
    sql: ${TABLE}.successful_mandates ;;
  }

  set: detail {
    fields: [created_date, payee_vpa, payee_name, successful_mandates]
  }
}
