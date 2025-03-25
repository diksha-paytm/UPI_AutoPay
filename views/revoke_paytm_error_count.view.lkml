view: revoke_paytm_error_count {
  derived_table: {
    sql: WITH final_status AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              COALESCE(NULLIF(ti.npci_resp_code, ''), 'NULL') AS npci_resp_code,
              Ctxn_id AS combi,
              MAX(ti.status) AS final_status
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND first_phase = 'ReqMandate-PAYER'
              AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'REVOKE'
              AND SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) = 'paytm'

GROUP BY 1, 2, 3
      ),
      failure_data AS (
          -- Count only those where the final status = 'FAILURE'
          SELECT
              fs.created_date,
              fs.npci_resp_code,
              COUNT(DISTINCT fs.combi) AS failure_count
          FROM final_status fs
          WHERE fs.final_status = 'FAILURE'
          GROUP BY 1, 2
      ),
      latest_failures AS (
          -- Identify the latest day's failure data
          SELECT created_date
          FROM failure_data
          ORDER BY created_date DESC
          LIMIT 1
      ),
      top_10_codes AS (
          -- Find the top 10 failure response codes for Paytm on the latest day
          SELECT
              fd.npci_resp_code
          FROM failure_data fd
          JOIN latest_failures lf
              ON fd.created_date = lf.created_date
          ORDER BY fd.failure_count DESC
          LIMIT 10
      ),
      daily_total_failures AS (
          -- Compute total failures for Paytm on each day
          SELECT created_date, SUM(failure_count) AS total_failures
          FROM failure_data
          GROUP BY created_date
      )
      SELECT
          fd.created_date,
          fd.npci_resp_code,
          fd.failure_count AS count,
          dtf.total_failures AS total
      FROM failure_data fd
      JOIN top_10_codes t10
          ON fd.npci_resp_code = t10.npci_resp_code
      JOIN daily_total_failures dtf
          ON fd.created_date = dtf.created_date
      ORDER BY fd.created_date DESC, fd.failure_count DESC
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

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
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
    fields: [created_date, npci_resp_code, count_, total]
  }
}
