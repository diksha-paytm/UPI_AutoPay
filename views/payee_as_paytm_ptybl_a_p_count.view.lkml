view: payee_as_paytm_ptybl_a_p_count {
  derived_table: {
    sql: SELECT

                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
                         THEN si.umn
                         ELSE NULL
                     END) AS paytm_active,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptaxis_active,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
                         THEN si.umn
                         ELSE NULL
                     END) AS pthdfc_active,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptsbi_active,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptyes_active,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
                         THEN si.umn
                         ELSE NULL
                     END) AS paytm_paused,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptaxis_paused,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
                         THEN si.umn
                         ELSE NULL
                     END) AS pthdfc_paused,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptsbi_paused,
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
                         THEN si.umn
                         ELSE NULL
                     END) AS ptyes_paused,
                     -- Adding Total Column
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'ACTIVE' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
                         THEN si.umn
                         ELSE NULL
                     END) AS total_active,
                     -- Adding Total Column
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
                         THEN si.umn
                         ELSE NULL
                     END) +
                     COUNT(DISTINCT CASE
                         WHEN si.mandate_status = 'PAUSED' AND SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
                         THEN si.umn
                         ELSE NULL
                     END) AS total_paused

      FROM
      hive.switch.standing_instructions_snapshot_v3 si
      JOIN hive.switch.standing_instructions_participants_snapshot_v3 sip
      ON si.umn = sip.umn
      WHERE
      si.mandate_status IN ('ACTIVE','PAUSED')
      AND si.dl_last_updated > DATE('2024-03-01')
      AND sip.dl_last_updated > DATE('2024-03-01')
      AND sip.participant_type = 'PAYEE'
      AND SUBSTRING(sip.vpa FROM POSITION('@' IN sip.vpa) + 1) in ('paytm','ptybl')
      ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: paytm_active {
    type: number
    sql: ${TABLE}.paytm_active ;;
  }

  dimension: ptaxis_active {
    type: number
    sql: ${TABLE}.ptaxis_active ;;
  }

  dimension: pthdfc_active {
    type: number
    sql: ${TABLE}.pthdfc_active ;;
  }

  dimension: ptsbi_active {
    type: number
    sql: ${TABLE}.ptsbi_active ;;
  }

  dimension: ptyes_active {
    type: number
    sql: ${TABLE}.ptyes_active ;;
  }

  dimension: paytm_paused {
    type: number
    sql: ${TABLE}.paytm_paused ;;
  }

  dimension: ptaxis_paused {
    type: number
    sql: ${TABLE}.ptaxis_paused ;;
  }

  dimension: pthdfc_paused {
    type: number
    sql: ${TABLE}.pthdfc_paused ;;
  }

  dimension: ptsbi_paused {
    type: number
    sql: ${TABLE}.ptsbi_paused ;;
  }

  dimension: ptyes_paused {
    type: number
    sql: ${TABLE}.ptyes_paused ;;
  }

  dimension: total_active {
    type: number
    sql: ${TABLE}.total_active ;;
  }

  dimension: total_paused {
    type: number
    sql: ${TABLE}.total_paused ;;
  }

  set: detail {
    fields: [
      paytm_active,
      ptaxis_active,
      pthdfc_active,
      ptsbi_active,
      ptyes_active,
      paytm_paused,
      ptaxis_paused,
      pthdfc_paused,
      ptsbi_paused,
      ptyes_paused,
      total_active,
      total_paused
    ]
  }
}
