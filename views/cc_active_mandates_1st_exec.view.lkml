view: cc_active_mandates_1st_exec {
  derived_table: {
    sql: SELECT
        count(distinct si.umn) as active_mandates
        FROM
        hive.switch.standing_instructions_snapshot_v3 si
        join hive.switch.standing_instructions_participants_snapshot_v3 sip on si.umn = sip.umn
        join hive.switch.txn_info_snapshot_v3 ti on si.umn = ti.umn
      WHERE
        si.mandate_status = 'ACTIVE'
        and ti.status= 'SUCCESS'
        and ti.type = 'COLLECT'
        and  cast(
    replace(
      json_query(
        ti.extended_info,
        'strict $.MANDATE_EXECUTION_NUMBER'
      ),
      '"',
      ''
    ) as integer
  ) = 1
        and sip.account_type = 'CREDIT'
        and REGEXP_EXTRACT(
          'upi://mandate?purpose=14&mc=8398&txnType=CREATE&validitystart=16032024',
          'purpose=([^&]*)', 1
      )= '14'
        AND si.dl_last_updated > DATE('2024-03-01')
          AND sip.dl_last_updated > DATE('2024-03-01')
          AND ti.dl_last_updated > DATE('2024-03-01')
 ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: active_mandates {
    type: number
    sql: ${TABLE}.active_mandates ;;
  }

  set: detail {
    fields: [active_mandates]
  }
}
