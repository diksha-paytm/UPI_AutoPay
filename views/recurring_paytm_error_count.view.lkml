view: recurring_paytm_error_count {
  derived_table: {
    sql: WITH ranked_txns AS (
    -- Rank transactions to get the latest status for each UMN & execution number
    SELECT
        DATE(ti.created_on) AS created_date,
        ti.umn,
        REPLACE(
            JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
            '"', ''
        ) AS exec_no,
        COALESCE(NULLIF(ti.npci_resp_code, ''), 'NULL') AS npci_resp_code,
        ti.status,
        ROW_NUMBER() OVER (
            PARTITION BY ti.umn,
                         REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '')
            ORDER BY ti.created_on DESC  -- Pick latest transaction per UMN & exec_no
        ) AS rn
    FROM hive.switch.txn_info_snapshot_v3 ti
    WHERE
        ti.business_type = 'MANDATE'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
        AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
        AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
        AND ti.type = 'COLLECT'
        AND CAST(REPLACE(JSON_QUERY(ti.extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
        AND SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) = 'paytm'
),
final_failures AS (
    -- Select only the final (latest) transaction where status = FAILURE
    SELECT created_date, npci_resp_code, COUNT(DISTINCT umn) AS failure
    FROM ranked_txns
    WHERE rn = 1  -- Keep only the latest txn for each umn+exec_no
    AND status = 'FAILURE'
    GROUP BY 1, 2
),
latest_failures AS (
    -- Find the latest date available
    SELECT MAX(created_date) AS latest_date FROM final_failures
),
top_5_codes AS (
    -- Get top 10 failure reasons for the latest date
    SELECT npci_resp_code
    FROM final_failures ff
    JOIN latest_failures lf
        ON ff.created_date = lf.latest_date
    ORDER BY ff.failure DESC
    LIMIT 10
),
daily_total_failures AS (
    -- Compute total failures per day
    SELECT created_date, SUM(failure) AS total_failures
    FROM final_failures
    GROUP BY created_date
)
SELECT
    ff.created_date,
    ff.npci_resp_code,
    ff.failure AS count,
    dtf.total_failures AS total
FROM final_failures ff
JOIN latest_failures lf
    ON ff.created_date = lf.latest_date  -- Filter to latest day's failures
JOIN top_5_codes t5
    ON ff.npci_resp_code = t5.npci_resp_code
JOIN daily_total_failures dtf
    ON ff.created_date = dtf.created_date
ORDER BY ff.failure DESC

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
