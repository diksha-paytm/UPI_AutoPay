view: payer_revokes_error {
  derived_table: {
    sql: WITH ranked_data AS (
        SELECT
          DATE(created_on) AS created_date,
          SUBSTRING(
            umn
            FROM
              POSITION('@' IN umn) + 1
          ) AS handle,
          npci_resp_code,
          COUNT(DISTINCT ti.txn_id) AS failure_count,
          RANK() OVER (
            PARTITION BY DATE(created_on), SUBSTRING(
              umn
              FROM
                POSITION('@' IN umn) + 1
            )
            ORDER BY COUNT(DISTINCT ti.txn_id) DESC
          ) AS rank
        FROM
          hive.switch.txn_info_snapshot_v3 ti
        WHERE
          business_type = 'MANDATE'
          AND JSON_QUERY(extended_info, 'strict $.purpose') = '"14"'
          AND type = 'REVOKE'
          AND dl_last_updated IS NOT NULL
          AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
          AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
          AND status = 'FAILURE'
          and first_phase != 'REQMANDATECONFIRMATION-REVOKE'
        GROUP BY
          DATE(created_on),
          SUBSTRING(
            umn
            FROM
              POSITION('@' IN umn) + 1
          ),
          npci_resp_code
      )
      SELECT
        created_date,
        handle,
        npci_resp_code,
        failure_count
      FROM
        ranked_data
      WHERE
        rank <= 3
      ORDER BY
        created_date DESC,
        handle,
        failure_count DESC
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

  dimension: handle {
    type: string
    sql: ${TABLE}.handle ;;
  }

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: failure_count {
    type: number
    sql: ${TABLE}.failure_count ;;
  }

  set: detail {
    fields: [created_date, handle, npci_resp_code, failure_count]
  }
}
