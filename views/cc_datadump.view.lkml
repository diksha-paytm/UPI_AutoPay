view: cc_datadump {
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
        ti.npci_resp_code,
        tp.name AS payer_name,
        tp1.name AS payee_name,
        tp1.bank_code as bene_bank,
  tp.bank_code as rem_bank,
        tp.vpa AS payer_vpa,
        tp1.vpa AS payee_vpa,
        replace(
              json_query(
                ti.extended_info,
                'strict $.MANDATE_EXECUTION_NUMBER'
              ),
              '"',
              ''
            ) as exec_no,
        ti.first_phase,
        ti.umn as umn
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
        AND ti.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
        AND tp.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
        AND tp1.dl_last_updated >= DATE_ADD('day', -50,CURRENT_DATE)
        AND tp.participant_type = 'PAYER'
        AND tp1.participant_type = 'PAYEE'
        AND ti.created_on >= CAST(DATE_ADD('day', -50,CURRENT_DATE) AS TIMESTAMP)
        ORDER BY
        1 desc
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

  dimension: bene_bank {
    type: string
    sql: ${TABLE}.bene_bank ;;
  }

  dimension: rem_bank {
    type: string
    sql: ${TABLE}.rem_bank ;;
  }

  dimension: payer_vpa {
    type: string
    sql: ${TABLE}.payer_vpa ;;
  }

  dimension: payee_vpa {
    type: string
    sql: ${TABLE}.payee_vpa ;;
  }

  dimension: exec_no {
    type: string
    sql: ${TABLE}.exec_no ;;
  }

  dimension: first_phase {
    type: string
    sql: ${TABLE}.first_phase ;;
  }

  dimension: umn {
    type: string
    sql: ${TABLE}.umn ;;
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
      npci_resp_code,
      payer_name,
      payee_name,
      bene_bank,
      rem_bank,
      payer_vpa,
      payee_vpa,
      exec_no,
      first_phase,
      umn
    ]
  }
}
