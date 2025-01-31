view: cc_top_merchants {
  derived_table: {
    sql: WITH payee_data AS (
    SELECT
        tp1.name AS payee_name,
        tp1.vpa AS payee_vpa,
        -- Count successful 'CREATE' transactions
        COUNT(CASE WHEN ti.type = 'CREATE' THEN 1 END) AS create_success,
        -- Count successful 'COLLECT' transactions
        COUNT(CASE WHEN ti.type = 'COLLECT' THEN 1 END) AS collect_success,
        -- Count successful 'REVOKE' transactions
        COUNT(CASE WHEN ti.type = 'REVOKE' THEN 1 END) AS revoke_success,
        -- Count all successful transactions
        COUNT(*) AS total_success_count
    FROM
        hive.switch.txn_info_snapshot_v3 ti
        JOIN hive.switch.txn_participants_snapshot_v3 tp
            ON ti.txn_id = tp.txn_id
        JOIN hive.switch.txn_participants_snapshot_v3 tp1
            ON ti.txn_id = tp1.txn_id
    WHERE
        ti.business_type = 'MANDATE'
        AND tp.account_type = 'CREDIT'
        AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
        AND ti.dl_last_updated >= DATE_ADD('day', -100, CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -100, CURRENT_DATE)
        AND tp1.dl_last_updated >= DATE_ADD('day', -100, CURRENT_DATE)
        AND tp.participant_type = 'PAYER'
        AND tp1.participant_type = 'PAYEE'
        AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
        AND ti.status = 'SUCCESS' -- Only count successful transactions
    GROUP BY
        tp1.name,
        tp1.vpa
),
top_30_vpas AS (
    SELECT
        payee_name,
        payee_vpa,
        create_success,
        collect_success,
        revoke_success,
        total_success_count
    FROM
        payee_data
    ORDER BY
        total_success_count DESC
    LIMIT 30 -- Select only the top 30 VPAs
)
SELECT
    payee_name,
    payee_vpa,
    create_success,
    collect_success,
    revoke_success,
    total_success_count
FROM
    top_30_vpas
ORDER BY
    total_success_count DESC;

       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: payee_name {
    type: string
    sql: ${TABLE}.payee_name ;;
  }

  dimension: payee_vpa {
    type: string
    sql: ${TABLE}.payee_vpa ;;
  }

  dimension: total_success_count {
    type: number
    sql: ${TABLE}.total_success_count ;;
  }

  set: detail {
    fields: [payee_name, payee_vpa, total_success_count]
  }
}
