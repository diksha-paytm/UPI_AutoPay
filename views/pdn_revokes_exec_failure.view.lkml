view: pdn_revokes_exec_failure {
  derived_table: {
    sql: WITH pdn_data AS (
    -- Extract FN date, UMN, Mandate Execution Date, and Handle
    SELECT
        DATE(fn.created_on) AS fn_date,  -- FN Created Date (Date Format)
        fn.created_on AS fn_created_on, -- FN Created Timestamp
        fn.txn_ref_id AS umn,           -- Unique Mandate Number (UMN)
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REPLACE(JSON_QUERY(fn.request_metadata, 'strict $.txnDetail.mandateExecutionDate'), '"', ''),
                'T', ' '
            ),
            '[+-][0-9]{2}:[0-9]{2}$', ''
        ) AS mandate_exec_date,  -- Mandate Execution Date
        SUBSTRING(fn.txn_ref_id FROM POSITION('@' IN fn.txn_ref_id) + 1) AS handle -- Extract Handle
    FROM hive.switch.financial_notification_snapshot_v3 fn
    WHERE fn.status = 'SUCCESS'
        AND fn.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
              AND created_on >= CAST(DATE_ADD('day', -30, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
),

revoke_check AS (
    -- ✅ Find revokes that happened strictly between FN creation and mandate execution date
    SELECT DISTINCT
        pdn.fn_date,
        pdn.handle,
        ti.txn_id
    FROM pdn_data pdn
    JOIN hive.switch.txn_info_snapshot_v3 ti
        ON ti.umn = pdn.umn
    WHERE ti.type = 'REVOKE'
        AND ti.first_phase = 'ReqMandate-PAYER'
        AND ti.business_type = 'MANDATE'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.status = 'FAILURE'
        AND ti.dl_last_updated >= DATE_ADD('day', -30, CURRENT_DATE)
        -- ✅ Allow revokes at any time between FN creation and Mandate Execution Date
        AND ti.created_on BETWEEN CAST(pdn.fn_created_on AS TIMESTAMP)
                              AND CAST(pdn.mandate_exec_date AS TIMESTAMP)
)

-- Final Aggregation: Count revoked UMNs handle-wise
SELECT
    fn_date,  -- ✅ FN Creation Date (Grouping Key)
    COUNT(DISTINCT CASE WHEN handle = 'paytm' THEN txn_id END) AS paytm,
    COUNT(DISTINCT CASE WHEN handle = 'ptaxis' THEN txn_id END) AS ptaxis,
    COUNT(DISTINCT CASE WHEN handle = 'pthdfc' THEN txn_id END) AS pthdfc,
    COUNT(DISTINCT CASE WHEN handle = 'ptsbi' THEN txn_id END) AS ptsbi,
    COUNT(DISTINCT CASE WHEN handle = 'ptyes' THEN txn_id END) AS ptyes,
    COUNT(DISTINCT txn_id) AS total -- ✅ Total revoked UMNs across all handles
FROM revoke_check
GROUP BY fn_date
ORDER BY fn_date DESC

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
