view: cc_1st_exec_payee_vpa_name_success {
  derived_table: {
    sql: WITH latest_successful_txns AS (
          -- Get latest transaction date with at least one success
          SELECT MAX(created_date) AS latest_date
          FROM (
              SELECT DATE(created_on) AS created_date
              FROM hive.switch.txn_info_snapshot_v3
              WHERE dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
                  AND created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
                  AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
                  ) t
      ),
      latest_date_payees AS (
          -- Get Payee VPA and Name from transactions on the latest date
          SELECT DISTINCT
              tp.vpa AS payee_vpa,
              tp.name AS payee_name
          FROM hive.switch.txn_info_snapshot_v3 ti
          JOIN hive.switch.txn_participants_snapshot_v3 tp
              ON ti.txn_id = tp.txn_id
          WHERE
              ti.business_type = 'MANDATE'
              AND tp.account_type = 'CREDIT'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND ti.status = 'SUCCESS'
              AND tp.participant_type = 'PAYEE'
              AND DATE(ti.created_on) = (SELECT latest_date FROM latest_successful_txns)
      ),

      all_dates_successful_txns AS (
      -- Fetch successful transactions for all dates but only for payees from the latest date
      SELECT
      DATE(ti.created_on) AS created_date,
      tp.vpa AS payee_vpa,
      tp.name payee_name,
      COUNT(DISTINCT ti.umn) AS successful_mandates
      FROM hive.switch.txn_info_snapshot_v3 ti
      JOIN hive.switch.txn_participants_snapshot_v3 tp
      ON ti.txn_id = tp.txn_id
      JOIN latest_date_payees lp
      ON tp.vpa = lp.payee_vpa AND tp.name = lp.payee_name
      WHERE
      ti.business_type = 'MANDATE'
              AND tp.account_type = 'CREDIT'
      AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
      AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND tp.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
      AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
      AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
      AND ti.type = 'COLLECT'
      AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
      AND ti.status = 'SUCCESS'
      AND tp.participant_type = 'PAYEE'
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
