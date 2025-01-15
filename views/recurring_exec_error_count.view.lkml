view: 1_exec_error_count {
  derived_table: {
    sql: select
        date(ti.created_on) as created_on,
        SUBSTRING(
          ti.umn
          FROM
            POSITION('@' IN ti.umn) + 1
        ) AS handle,
        npci_resp_code,
       count(
          distinct concat(
            umn,
            replace(
              json_query(
                extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            )
          )
        ) as count_
      from
        hive.switch.txn_info_snapshot_v3 ti
      where
        ti.business_type = 'MANDATE'
        and json_query(ti.extended_info, 'strict$.purpose') = '"14"'
        and cast(
          replace(
            json_query(
              extended_info,
              'strict $.MANDATE_EXECUTION_NUMBER'
            ),
            '"',
            ''
          ) as integer
        ) > 1
        AND dl_last_updated IS NOT NULL
        AND created_on >= CAST(DATE_ADD('day', -100,CURRENT_DATE) AS TIMESTAMP) -- Start 100 days before today
        AND created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
        and ti.type = 'COLLECT'
        and ti.status = 'FAILURE'
        and ((SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'paytm' and npci_resp_code in ('U30-Z9','U30-Z8',
            'U28'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptaxis' and npci_resp_code in ('U30-Z9','U67-UT'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'pthdfc' and npci_resp_code in ('U30-Z9','U67-UT'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptsbi' and npci_resp_code in ('U30-Z9','U30-Z8'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptyes' and npci_resp_code in ('U30-Z9','U30-B3')))
      group by
        1,
        2,
        3
      order by
        1 desc,
        2,
        3
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: created_on {
    type: date
    sql: ${TABLE}.created_on ;;
  }

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: count_ {
    type: number
    sql: ${TABLE}.count_ ;;
  }

  set: detail {
    fields: [created_on, handle, npci_resp_code, count_]
  }
}
