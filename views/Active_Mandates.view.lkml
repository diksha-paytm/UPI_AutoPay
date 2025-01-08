view: active_mandates {
  derived_table: {
    sql: WITH active_mandates AS (select *
      from
      (SELECT count(distinct umn) active_mandates FROM "hive"."switch"."standing_instructions_snapshot_v3"
            where dl_last_updated is not null
            and mandate_status = 'ACTIVE'
            ) a
      left join
      (
      select count(distinct rev.scope_cust_id) users_who_revoked_mandate
              , count(distinct case when act.scope_cust_id is null then rev.scope_cust_id end) users_who_never_activated_again
      from
      (
       SELECT scope_cust_id, min(si.created_on) min_revoked
       from
      (
      SELECT *
       FROM "hive"."switch"."standing_instructions_snapshot_v3"
            where dl_last_updated is not null
          --   and mandate_status = 'ACTIVE'
            and type = 'REVOKE') si
      left join
      (SELECT * FROM "hive"."switch"."standing_instructions_participants_snapshot_v3"
      where dl_last_updated  is not null
       ) mp on si.txn_id = mp.txn_id
      group by 1
      ) rev
      left join
      ( SELECT scope_cust_id, max(si.created_on) max_created
       from
      (
      SELECT *
       FROM "hive"."switch"."standing_instructions_snapshot_v3"
            where dl_last_updated is not null
          --   and mandate_status = 'ACTIVE'
          and type = 'CREATE'
            ) si
      left join
      (SELECT * FROM "hive"."switch"."standing_instructions_participants_snapshot_v3"
      where dl_last_updated  is not null
       ) mp on si.txn_id = mp.txn_id
      group by 1
      ) act on rev.scope_cust_id = act.scope_cust_id and max_created>min_revoked
      ) b

      on 1=1
      )
      SELECT
      COALESCE(SUM(active_mandates.active_mandates), 0) AS "Active_Mandates"
      FROM active_mandates
      LIMIT 500
      ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: active_mandates {
    type: number
    sql: ${TABLE}.Active_Mandates ;;
  }

  set: detail {
    fields: [active_mandates]
  }
}
