view: cc_active_mandates {
  derived_table: {
    sql: SELECT
        count(distinct si.umn) as active_mandates
        FROM
        hive.switch.standing_instructions_snapshot_v3 si
        join hive.switch.standing_instructions_participants_snapshot_v3 sip on si.umn = sip.umn
      WHERE
        si.mandate_status = 'ACTIVE'
        and sip.account_type = 'CREDIT'
        and REGEXP_EXTRACT(
          'upi://mandate?purpose=14&mc=8398&txnType=CREATE&validitystart=16032024',
          'purpose=([^&]*)', 1
      )= '14'
        AND si.dl_last_updated > DATE('2024-03-01')
          AND sip.dl_last_updated > DATE('2024-03-01')
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
