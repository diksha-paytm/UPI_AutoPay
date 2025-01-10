view: creations_error_count {
  derived_table: {
    sql: select
        date(ti.created_on) as Created_on,
        SUBSTRING(
          ti.umn
          FROM
            POSITION('@' IN ti.umn) + 1
        ) AS handle,
        replace(
          json_query(ti.extended_info, 'strict $.initiationMode'),
          '"',
          ''
        ) initiation_mode,
        npci_resp_code,
        count(distinct ti.umn) as Error_Count
      from
        hive.switch.txn_info_snapshot_v3 ti
      where
        ti.business_type = 'MANDATE'
        and json_query(ti.extended_info, 'strict$.purpose') = '"14"'
        AND dl_last_updated IS NOT NULL
        AND created_on >= CAST(DATE_ADD('day', -100,CURRENT_DATE) AS TIMESTAMP) -- Start 100 days before today
        AND created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
        and ti.type = 'CREATE'
        and ti.status = 'FAILURE'
        and ((SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptaxis' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '00'
              and npci_resp_code in ('UM3','ZA','UM8-ZM','ZM-ZM'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptaxis' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '04'
              and npci_resp_code in ('UM8-ZM','UM8','UM1','ZM-ZM','UM1-UM1'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptaxis' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '13'
              and npci_resp_code in ('UM1','UM8-ZM','UM9','UM1-UM1','ZM-ZM'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'pthdfc' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '00'
              and npci_resp_code in ('UM3','MD00','U66'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'pthdfc' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '04'
              and npci_resp_code in ('UM8-ZM','UM9','UM8-Z6'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'pthdfc' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '13'
              and npci_resp_code in ('U66','UM1','UM8-ZM'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptsbi' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '00'
              and npci_resp_code in ('UM3','MD00','UM8-ZM'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptsbi' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '04'
              and npci_resp_code in ('UM8-ZM','UM8-Z6','UM9'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptsbi' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '13'
              and npci_resp_code in ('UM1','UM8-ZM','UM2'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptyes' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '00'
              and npci_resp_code in ('UM3','ZA','UM8-ZM'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptyes' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '04'
              and npci_resp_code in ('UM8-ZM','UM8','UM1'))
            or (SUBSTRING(umn FROM POSITION('@' IN umn) + 1)= 'ptyes' and replace(json_query(ti.extended_info, 'strict $.initiationMode'),'"','')= '13'
              and npci_resp_code in ('UM1','UM8-ZM','UM9'))
        )
      group by
        1,
        2,
        3,
        4
      order by
        1 desc,
        2,
        3,5 desc
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: created_on {
    type: date
    sql: ${TABLE}.Created_on ;;
  }

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: initiation_mode {
    type: string
    sql: ${TABLE}.initiation_mode ;;
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: error_count {
    type: number
    sql: ${TABLE}.Error_Count ;;
  }

  set: detail {
    fields: [created_on, handle, initiation_mode, npci_resp_code, error_count]
  }
}
