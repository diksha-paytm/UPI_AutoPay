view: active_mandates {
  derived_table: {
    sql: WITH active_mandates AS (
        SELECT *
        FROM (
          SELECT
            COUNT(DISTINCT si.umn) AS active_mandates,
            COUNT(DISTINCT json_query(sip.extended_info, 'strict $.payerCustId')) AS distinct_users_with_active_mandates
          FROM hive.switch.standing_instructions_snapshot_v3 si
          JOIN hive.switch.standing_instructions_participants_snapshot_v3 sip
            ON si.umn = sip.umn
          WHERE si.dl_last_updated > DATE('2024-03-01')
            AND sip.dl_last_updated > DATE('2024-03-01')
            AND si.mandate_status = 'ACTIVE'
        ) a
        LEFT JOIN (
          SELECT
            COUNT(DISTINCT rev.payer_cust_id) AS users_who_revoked_mandate,
            COUNT(DISTINCT CASE
              WHEN act.max_created IS NULL THEN rev.payer_cust_id
            END) AS users_who_never_activated_again,
            COUNT(DISTINCT CASE
              WHEN act.max_created IS NOT NULL THEN rev.payer_cust_id
            END) AS users_who_activated_again
          FROM (
            SELECT
              json_query(sip.extended_info, 'strict $.payerCustId') AS payer_cust_id,
              MIN(si.created_on) AS min_revoked
            FROM hive.switch.standing_instructions_snapshot_v3 si
            JOIN hive.switch.standing_instructions_participants_snapshot_v3 sip
              ON si.umn = sip.umn
            WHERE si.dl_last_updated > DATE('2024-03-01')
              AND si.type = 'REVOKE'
              AND sip.dl_last_updated > DATE('2024-03-01')
            GROUP BY json_query(sip.extended_info, 'strict $.payerCustId')
          ) rev
          LEFT JOIN (
            SELECT
              json_query(sip.extended_info, 'strict $.payerCustId') AS payer_cust_id,
              MAX(si.created_on) AS max_created
            FROM hive.switch.standing_instructions_snapshot_v3 si
            JOIN hive.switch.standing_instructions_participants_snapshot_v3 sip
              ON si.umn = sip.umn
            WHERE si.dl_last_updated > DATE('2024-03-01')
              AND si.type = 'CREATE'
              AND sip.dl_last_updated > DATE('2024-03-01')
            GROUP BY json_query(sip.extended_info, 'strict $.payerCustId')
          ) act
          ON rev.payer_cust_id = act.payer_cust_id AND act.max_created > rev.min_revoked
        ) b ON 1=1
      )
     SELECT
    CONCAT(CAST(COALESCE(SUM(active_mandates.active_mandates), 0) / 1000000.00 AS VARCHAR), ' Mn') AS "Active_Mandates",
    CONCAT(CAST(COALESCE(SUM(active_mandates.distinct_users_with_active_mandates), 0) / 1000000.00 AS VARCHAR), ' Mn') AS "Distinct_Users_With_Active_Mandates",
    CONCAT(CAST(COALESCE(SUM(active_mandates.users_who_revoked_mandate), 0) / 1000000.00 AS VARCHAR), ' Mn') AS "Users_Who_Revoked_Mandate",
    CONCAT(CAST(COALESCE(SUM(active_mandates.users_who_never_activated_again), 0) / 1000000.00 AS VARCHAR), ' Mn') AS "Users_Who_Never_Activated_Again",
    CONCAT(CAST(COALESCE(SUM(active_mandates.users_who_activated_again), 0) / 1000000.00 AS VARCHAR), ' Mn') AS "Users_Who_Activated_Again"
FROM active_mandates
 ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: active_mandates {
    type: string
    sql: ${TABLE}.Active_Mandates ;;
  }

  dimension: distinct_users_with_active_mandates {
    type: string
    sql: ${TABLE}.Distinct_Users_With_Active_Mandates ;;
  }

  dimension: users_who_revoked_mandate {
    type: string
    sql: ${TABLE}.Users_Who_Revoked_Mandate ;;
  }

  dimension: users_who_never_activated_again {
    type: string
    sql: ${TABLE}.Users_Who_Never_Activated_Again ;;
  }

  dimension: users_who_activated_again {
    type: string
    sql: ${TABLE}.Users_Who_Activated_Again ;;
  }

  set: detail {
    fields: [active_mandates, distinct_users_with_active_mandates, users_who_revoked_mandate, users_who_never_activated_again, users_who_activated_again]
  }
}
