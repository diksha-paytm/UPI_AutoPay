view: cc_1st_exec_error_contri {
  derived_table: {
    sql: WITH failures AS (
    SELECT
        DATE(ti.created_on) AS created_date,
        COALESCE(NULLIF(ti.npci_resp_code, ''), 'NULL') AS npci_resp_code,
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
    JOIN hive.switch.txn_participants_snapshot_v3 tp
        ON ti.txn_id = tp.txn_id
    WHERE
        ti.business_type = 'MANDATE'
        AND tp.account_type = 'CREDIT'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
        AND ti.type = 'COLLECT'
        AND ti.status = 'FAILURE'
        AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
    GROUP BY 1, 2
),
latest_failures AS (
    -- Identify the latest day's failure data
    SELECT created_date
    FROM failures
    ORDER BY created_date DESC
    LIMIT 1
),
top_10_codes AS (
    -- Find the top 10 failure response codes for Paytm on the latest day
    SELECT pf.npci_resp_code
    FROM failures pf
    JOIN latest_failures lf
        ON pf.created_date = lf.created_date
    ORDER BY pf.failure DESC
    LIMIT 10
),
daily_total_txns AS (
    -- Compute total transactions (success + failure) for Paytm on each day
    SELECT
        DATE(ti.created_on) AS created_date,
        COUNT(
            DISTINCT CONCAT(
                ti.umn,
                REPLACE(
                    JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                    '"',
                    ''
                )
            )
        ) AS total_txns
    FROM hive.switch.txn_info_snapshot_v3 ti
    JOIN hive.switch.txn_participants_snapshot_v3 tp
        ON ti.txn_id = tp.txn_id
    WHERE
        ti.business_type = 'MANDATE'
        AND tp.account_type = 'CREDIT'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
        AND ti.type = 'COLLECT'
        AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
    GROUP BY 1
)
SELECT
    pf.created_date,
    pf.npci_resp_code AS "Error Code",
    CAST(ROUND(100.0 * pf.failure / NULLIF(dt.total_txns, 0), 2) AS VARCHAR) || '%' AS "Failure Percentage"
FROM failures pf
JOIN top_10_codes t10
    ON pf.npci_resp_code = t10.npci_resp_code
JOIN daily_total_txns dt
    ON pf.created_date = dt.created_date
ORDER BY pf.created_date DESC, pf.failure DESC;

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

  dimension: error_code {
    type: string
    label: "Error Code"
    sql: ${TABLE}."Error Code" ;;
  }

  dimension: failure_contribution {
    type: string
    label: "Failure Contribution"
    sql: ${TABLE}."Failure Contribution" ;;
  }

  set: detail {
    fields: [created_date, error_code, failure_contribution]
  }
}
