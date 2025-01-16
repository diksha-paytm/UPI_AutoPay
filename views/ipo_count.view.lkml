view: ipo_count {
  derived_table: {
    sql: SELECT
  DATE(ti.created_on) AS created_on,
  SUBSTRING(
    ti.umn
    FROM
      POSITION('@' IN ti.umn) + 1
  ) AS handle,

  -- Creations
  COUNT(DISTINCT CASE WHEN ti.type = 'CREATE' AND ti.status = 'SUCCESS' THEN ti.umn END) AS creation_success,
  COUNT(DISTINCT CASE WHEN ti.type = 'CREATE' AND ti.status = 'FAILURE' THEN ti.umn END) AS creation_failure,
   ROUND(
    COALESCE(
      COUNT(
        DISTINCT CASE
          WHEN ti.type = 'CREATE' AND status = 'SUCCESS' THEN ti.umn END
      ),
      0
    ) * 100.0 / NULLIF(
      COALESCE(
        COUNT(
          DISTINCT CASE
            WHEN ti.type = 'CREATE' AND status IN ('SUCCESS','FAILURE') THEN ti.umn END
        ),
        0
      ),
      0
    ),
    2
  )  AS creations_sr,

  -- Executions
  COUNT(DISTINCT CASE WHEN ti.type = 'COLLECT' AND ti.status = 'SUCCESS' THEN ti.umn END) AS execution_success,
  COUNT(DISTINCT CASE WHEN ti.type = 'COLLECT' AND ti.status = 'FAILURE' THEN ti.umn END) AS execution_failure,
   ROUND(
    COALESCE(
      COUNT(
        DISTINCT CASE
          WHEN ti.type = 'COLLECT' AND status = 'SUCCESS' THEN ti.umn END
      ),
      0
    ) * 100.0 / NULLIF(
      COALESCE(
        COUNT(
          DISTINCT CASE
            WHEN ti.type = 'COLLECT' AND status IN ('SUCCESS','FAILURE') THEN ti.umn END
        ),
        0
      ),
      0
    ),
    2
  ) AS executions_sr,

  -- Revokes
  COUNT(DISTINCT CASE WHEN ti.type = 'REVOKE' AND ti.status = 'SUCCESS' THEN ti.umn END) AS revoke_success
FROM
  hive.switch.txn_info_snapshot_v3 ti
  JOIN hive.switch.txn_participants_snapshot_v3 tp
    ON ti.txn_id = tp.txn_id
WHERE
  ti.dl_last_updated IS NOT NULL
  AND tp.dl_last_updated IS NOT NULL
  AND ti.created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP) -- Start 100 days before today
  AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP) -- End before today
  AND ti.status IN ('SUCCESS', 'FAILURE')
  AND ti.business_type = 'MANDATE'
  AND JSON_QUERY(ti.extended_info, 'strict $.purpose') = '"01"'
  AND tp.mcc = '6211'
  AND tp.participant_type = 'PAYEE'
GROUP BY
  1, 2
ORDER BY
  1 desc, 2

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

  dimension: creation_success {
    type: number
    sql: ${TABLE}.creation_success ;;
  }

  dimension: creation_failure {
    type: number
    sql: ${TABLE}.creation_failure ;;
  }

  dimension: creations_sr {
    type: number
    sql: ${TABLE}.creations_sr ;;
  }

  dimension: execution_success {
    type: number
    sql: ${TABLE}.execution_success ;;
  }

  dimension: execution_failure {
    type: number
    sql: ${TABLE}.execution_failure ;;
  }

  dimension: executions_sr {
    type: number
    sql: ${TABLE}.executions_sr ;;
  }

  dimension: revoke_success {
    type: number
    sql: ${TABLE}.revoke_success ;;
  }

  set: detail {
    fields: [
      created_on,
      handle,
      creation_success,
      creation_failure,
      creations_sr,
      execution_success,
      execution_failure,
      executions_sr,
      revoke_success
    ]
  }
}
