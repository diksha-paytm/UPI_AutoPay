view: creations_ptaxis_error_count {
  derived_table: {
    sql: WITH paytm_failures AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              -- Replace 'MD00' with 'ZA' before grouping
              COALESCE(NULLIF(CASE
                  WHEN ti.npci_resp_code = 'MD00' THEN 'ZA'
                  ELSE ti.npci_resp_code
              END, ''), 'NULL') AS npci_resp_code,
              COUNT(DISTINCT ti.umn) AS failure
          FROM team_product.looker_RM ti
          WHERE
              ti.type = 'CREATE'
              AND ti.status = 'FAILURE'
              AND SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1) = 'ptaxis'
          GROUP BY 1, 2
      ),
            latest_failures AS (
                -- Identify the latest day's failure data
                SELECT created_date
                FROM paytm_failures
                ORDER BY created_date DESC
                LIMIT 1
            ),
            top_5_codes AS (
                -- Find the top 5 failure response codes for Paytm on the latest day
                SELECT
                    pf.npci_resp_code
                FROM paytm_failures pf
                JOIN latest_failures lf
                    ON pf.created_date = lf.created_date
                ORDER BY pf.failure DESC
                LIMIT 10
            ),
            daily_total_failures AS (
                -- Compute total failures for Paytm on each day
                SELECT created_date, SUM(failure) AS total_failures
                FROM paytm_failures
                GROUP BY created_date
            )
            SELECT
                pf.created_date,
                pf.npci_resp_code,
                pf.failure AS count,
                dtf.total_failures AS total
            FROM paytm_failures pf
            JOIN top_5_codes t5
                ON pf.npci_resp_code = t5.npci_resp_code
            JOIN daily_total_failures dtf
                ON pf.created_date = dtf.created_date
            ORDER BY pf.created_date DESC, pf.failure DESC
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

  dimension: npci_resp_code {
    type: string
    sql: ${TABLE}.npci_resp_code ;;
  }

  dimension: count_ {
    type: number
    sql: ${TABLE}."count" ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  set: detail {
    fields: [created_date, npci_resp_code, count_, total]
  }
}
