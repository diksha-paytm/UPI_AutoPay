view: pdn_revokes_exec_failure {
  derived_table: {
    sql: WITH pdn_data AS (
          -- Extract mandate execution details from financial_notification
          SELECT
              DATE(fn.created_on) AS pdn_created_date,
              fn.txn_ref_id AS umn,
              CAST(JSON_QUERY(fn.request_metadata, 'strict $.txnDetail.mandateExecutionNo') AS INTEGER) AS mandate_execution_no,
              JSON_QUERY(fn.request_metadata, 'strict $.payer.scopeCustId') AS scope_cust_id,
              fn.created_on AS pdn_created_on
          FROM hive.switch.financial_notification_snapshot_v3 fn
          WHERE fn.status = 'SUCCESS'
              AND fn.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
                    AND fn.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
                    AND fn.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
      ),
      latest_execution AS (
          -- Join PDN data with txn_info to get the latest execution attempt
          SELECT
              pdn.pdn_created_date,
              pdn.umn,
              SUBSTRING(pdn.umn FROM POSITION('@' IN pdn.umn) + 1) AS handle, -- Extract handle
              pdn.mandate_execution_no,
              pdn.scope_cust_id,
              pdn.pdn_created_on,
              MAX(ti.created_on) AS exec_max_created_on
          FROM pdn_data pdn
          JOIN hive.switch.txn_info_snapshot_v3 ti
              ON CONCAT(ti.umn, REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', ''))
              = CONCAT(pdn.umn, CAST(pdn.mandate_execution_no AS VARCHAR))
          WHERE ti.type = 'COLLECT'
          AND ti.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
          GROUP BY 1, 2, 3, 4, 5, 6
      ),
      revoke_check AS (
          -- Check if revoke occurred between PDN created_on and latest execution created_on
          SELECT DISTINCT
              le.pdn_created_date,
              le.handle,
              le.scope_cust_id
          FROM latest_execution le
          JOIN hive.switch.txn_info_snapshot_v3 ti
              ON ti.umn = le.umn
          WHERE
              ti.type = 'REVOKE'
              AND ti.dl_last_updated >= DATE_ADD('day', -30,CURRENT_DATE)
              AND ti.first_phase = 'ReqMandate-PAYER'
              AND ti.status = 'FAILURE'
              AND ti.created_on BETWEEN le.pdn_created_on AND le.exec_max_created_on
      )
      -- Pivot data: Count users (scope_cust_id) per handle
      SELECT
          pdn_created_date,
          COUNT(DISTINCT CASE WHEN handle = 'paytm' THEN scope_cust_id END) AS paytm,
          COUNT(DISTINCT CASE WHEN handle = 'ptaxis' THEN scope_cust_id END) AS ptaxis,
          COUNT(DISTINCT CASE WHEN handle = 'pthdfc' THEN scope_cust_id END) AS pthdfc,
          COUNT(DISTINCT CASE WHEN handle = 'ptsbi' THEN scope_cust_id END) AS ptsbi,
          COUNT(DISTINCT CASE WHEN handle = 'ptyes' THEN scope_cust_id END) AS ptyes,
          COUNT(DISTINCT scope_cust_id) AS total -- Total count across all handles
      FROM revoke_check
      GROUP BY pdn_created_date
      ORDER BY pdn_created_date desc
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: pdn_created_date {
    type: date
    sql: ${TABLE}.pdn_created_date ;;
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
      pdn_created_date,
      paytm,
      ptaxis,
      pthdfc,
      ptsbi,
      ptyes,
      total
    ]
  }
}
