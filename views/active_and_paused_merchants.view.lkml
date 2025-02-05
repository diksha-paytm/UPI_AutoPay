view: active_and_paused_merchants {
  derived_table: {
    sql: SELECT
         sip.name,
         sip.vpa,
         si.mandate_status,
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
             THEN si.umn
             ELSE NULL
         END) AS paytm,
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
             THEN si.umn
             ELSE NULL
         END) AS ptaxis,
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
             THEN si.umn
             ELSE NULL
         END) AS pthdfc,
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
             THEN si.umn
             ELSE NULL
         END) AS ptsbi,
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
             THEN si.umn
             ELSE NULL
         END) AS ptyes,
         -- Adding Total Column
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'paytm'
             THEN si.umn
             ELSE NULL
         END) +
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptaxis'
             THEN si.umn
             ELSE NULL
         END) +
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'pthdfc'
             THEN si.umn
             ELSE NULL
         END) +
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptsbi'
             THEN si.umn
             ELSE NULL
         END) +
         COUNT(DISTINCT CASE
             WHEN SUBSTRING(si.umn FROM POSITION('@' IN si.umn) + 1) = 'ptyes'
             THEN si.umn
             ELSE NULL
         END) AS total
      FROM
         hive.switch.standing_instructions_snapshot_v3 si
         JOIN hive.switch.standing_instructions_participants_snapshot_v3 sip
           ON si.umn = sip.umn
      WHERE
         si.mandate_status IN ('ACTIVE','PAUSED')
         AND si.dl_last_updated > DATE('2024-03-01')
         AND sip.dl_last_updated > DATE('2024-03-01')
         AND sip.participant_type = 'PAYEE'
      GROUP BY
         1,2,3
      ORDER BY
         total DESC
      limit 200
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: vpa {
    type: string
    sql: ${TABLE}.vpa ;;
  }

  dimension: mandate_status {
    type: string
    sql: ${TABLE}.mandate_status ;;
  }

  dimension: paytm {
    type: number
    sql: ${TABLE}.paytm ;;
  }

  dimension: ptaxis {
    type: number
    sql: ${TABLE}.ptaxis ;;
  }

  dimension: pthdfc {
    type: number
    sql: ${TABLE}.pthdfc ;;
  }

  dimension: ptsbi {
    type: number
    sql: ${TABLE}.ptsbi ;;
  }

  dimension: ptyes {
    type: number
    sql: ${TABLE}.ptyes ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  set: detail {
    fields: [
      name,
      vpa,
      mandate_status,
      paytm,
      ptaxis,
      pthdfc,
      ptsbi,
      ptyes,
      total
    ]
  }
}
