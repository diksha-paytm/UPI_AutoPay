view: cc_mandates {
  derived_table: {
    sql: SELECT
        DATE(ti.created_on) AS created_date,
        CAST(ti.created_on AS TIMESTAMP) AS full_timestamp,
        ti.type,
        ti.status,
        SUBSTRING(
          ti.umn
          FROM
            POSITION('@' IN ti.umn) + 1
        ) AS handle,
        REPLACE(
          JSON_QUERY(ti.extended_info, 'strict $.initiationMode'),
          '"',
          ''
        ) AS initiation_mode,
        ti.txn_id,
        replace(
              json_query(
                ti.extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            ) as exec_no,
        ti.npci_resp_code,
        tp.name AS payer_name,
        tp1.name AS payee_name,
        tp.vpa AS payer_vpa,
        tp1.vpa AS payee_vpa,
        COUNT(DISTINCT ti.umn) AS umn_count
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
        AND ti.dl_last_updated > DATE('2024-12-01')
        AND tp.dl_last_updated > DATE('2024-12-01')
        AND tp1.dl_last_updated > DATE('2024-12-01')
        AND tp.participant_type = 'PAYER'
        AND tp1.participant_type = 'PAYEE'
        AND ti.created_on >= CAST('2024-12-31 00:00:00.000' AS TIMESTAMP)
      GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13
      ORDER BY
        1, 2, 3, 4, 5, 6, 7, 8,9,10,11,12,13
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

  dimension_group: full_timestamp {
    type: time
    sql: ${TABLE}.full_timestamp ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: initiation_mode {
    type: string
    sql: ${TABLE}.initiation_mode ;;
  }

  dimension: txn_id {
    type: string
    sql: ${TABLE}.txn_id ;;
  }

  dimension: exec_no {
    type: string
    sql: ${TABLE}.exec_no ;;
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: payer_name {
    type: string
    sql: ${TABLE}.payer_name ;;
  }

  dimension: payee_name {
    type: string
    sql: ${TABLE}.payee_name ;;
  }

  dimension: payer_vpa {
    type: string
    sql: ${TABLE}.payer_vpa ;;
  }

  dimension: payee_vpa {
    type: string
    sql: ${TABLE}.payee_vpa ;;
  }

  dimension: umn_count {
    type: number
    sql: ${TABLE}.umn_count ;;
  }

  set: detail {
    fields: [
      created_date,
      full_timestamp_time,
      type,
      status,
      handle,
      initiation_mode,
      txn_id,
      exec_no,
      npci_resp_code,
      payer_name,
      payee_name,
      payer_vpa,
      payee_vpa,
      umn_count
    ]
  }
}
