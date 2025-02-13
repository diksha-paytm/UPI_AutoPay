view: 1st_exec_ptaxis_error_count {
  derived_table: {
    sql: WITH ptaxis_failures AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              ti.npci_resp_code,
              COUNT(
            DISTINCT CONCAT(
                ti.umn,
                REPLACE(
                    JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                    '"',
                    ''
                )
            )
        ) AS failure
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND ti.status = 'FAILURE'
              AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) = 'ptaxis'
              and npci_resp_code is not null
          GROUP BY 1, 2
      ),
      latest_failures AS (
          -- Identify the latest day's failure data
          SELECT created_date
          FROM ptaxis_failures
          ORDER BY created_date DESC
          LIMIT 1
      ),
      top_5_codes AS (
          -- Find the top 5 failure response codes for Paytm on the latest day
          SELECT
              pf.npci_resp_code
          FROM ptaxis_failures pf
          JOIN latest_failures lf
              ON pf.created_date = lf.created_date
          ORDER BY pf.failure DESC
          LIMIT 5
      ),
      daily_total_failures AS (
          -- Compute total failures for Paytm on each day
          SELECT created_date, SUM(failure) AS total_failures
          FROM ptaxis_failures
          GROUP BY created_date
      )
      SELECT
          pf.created_date,
          pf.npci_resp_code,
          pf.failure AS count,
          dtf.total_failures AS total
      FROM ptaxis_failures pf
      JOIN top_5_codes t5
          ON pf.npci_resp_code = t5.npci_resp_code
      JOIN daily_total_failures dtf
          ON pf.created_date = dtf.created_date
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
