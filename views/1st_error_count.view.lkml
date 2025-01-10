view: 1st_error_count {
  derived_table: {
    sql: select
        date(created_on),
        SUBSTRING(
          umn
          FROM
            POSITION('@' IN umn) + 1
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
        )
      from
        hive.switch.txn_info_snapshot_v3
      where
        business_type = 'MANDATE'
        and json_query(extended_info, 'strict$.purpose') = '"14"'
        and cast(
          replace(
            json_query(
              extended_info,
              'strict $.MANDATE_EXECUTION_NUMBER'
            ),
            '"',
            ''
          ) as integer
        ) = 1
        AND dl_last_updated IS NOT NULL
          AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
          AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
          and type = 'COLLECT'
        and status = 'FAILURE'
        and ((SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'paytm' and npci_resp_code in ('NU'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptaxis' and npci_resp_code in ('U30-Z9','U30-YC'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'pthdfc' and npci_resp_code in ('U30-Z9','U30-YC'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptsbi' and npci_resp_code in ('U30-Z9','U30-Z8'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptyes' and npci_resp_code in ('U30-Z9','U30-Z8')))
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

  dimension: _col0 {
    type: date
    sql: ${TABLE}._col0 ;;
  }

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: _col3 {
    type: number
    sql: ${TABLE}._col3 ;;
  }

  set: detail {
    fields: [_col0, handle, npci_resp_code, _col3]
  }
}
