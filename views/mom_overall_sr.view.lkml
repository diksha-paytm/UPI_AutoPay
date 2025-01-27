view: mom_overall_sr {
  derived_table: {
    sql: WITH create_data AS (
          SELECT
              DATE_TRUNC('month', created_on) AS created_month,
              'CREATE' AS auto_pay_category,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'SUCCESS' THEN umn
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'FAILURE' THEN umn
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
              AND created_on < DATE_TRUNC('month', CURRENT_DATE)
              AND type = 'CREATE'
              AND SUBSTRING(umn FROM POSITION('@' IN umn) + 1) NOT IN ('PAYTM', 'PayTM', 'PayTm', 'Paytm', 'paytm')
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      first_execution_data AS (
          SELECT
              DATE_TRUNC('month', created_on) AS created_month,
              '1st Execution' AS auto_pay_category,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'SUCCESS' THEN CONCAT(
                          umn,
                          REPLACE(
                              JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                              '"',
                              ''
                          )
                      )
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'FAILURE' THEN CONCAT(
                          umn,
                          REPLACE(
                              JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                              '"',
                              ''
                          )
                      )
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
              AND created_on < DATE_TRUNC('month', CURRENT_DATE)
              AND type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      greater_execution_data AS (
          SELECT
              DATE_TRUNC('month', created_on) AS created_month,
              '>1 Executions' AS auto_pay_category,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'SUCCESS' THEN CONCAT(
                          umn,
                          REPLACE(
                              JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                              '"',
                              ''
                          )
                      )
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'FAILURE' THEN CONCAT(
                          umn,
                          REPLACE(
                              JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'),
                              '"',
                              ''
                          )
                      )
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND CAST(REPLACE(JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) > 1
              AND dl_last_updated IS NOT NULL
              AND created_on >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
              AND created_on < DATE_TRUNC('month', CURRENT_DATE)
              AND type = 'COLLECT'
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      revoke_data AS (
          SELECT
              DATE_TRUNC('month', created_on) AS created_month,
              'Revoke' AS auto_pay_category,
              COUNT(
                  CASE
                      WHEN status = 'SUCCESS' THEN txn_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'FAILURE' THEN txn_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
              AND created_on < DATE_TRUNC('month', CURRENT_DATE)
              AND type = 'REVOKE'
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      pdn_data AS (
          SELECT
              DATE_TRUNC('month', created_on) AS created_month,
              'PDN' AS auto_pay_category,
              COUNT(
                  CASE
                      WHEN status = 'SUCCESS' THEN 1
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  CASE
                      WHEN status = 'FAILURE' THEN 1
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.financial_notification_snapshot_v3 fn
          WHERE
              dl_last_updated IS NOT NULL
              AND created_on >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
              AND created_on < DATE_TRUNC('month', CURRENT_DATE)
          GROUP BY
              1
      )
      SELECT
          COALESCE(create_data.created_month, fe.created_month, ge.created_month, revoke_data.created_month, pdn_data.created_month) AS created_month,
          CONCAT(
              CAST(
                  ROUND(
                      MAX(CASE WHEN create_data.auto_pay_category = 'CREATE' THEN create_data.success ELSE NULL END) * 100.0 /
                      NULLIF(
                          MAX(CASE WHEN create_data.auto_pay_category = 'CREATE' THEN create_data.success ELSE 0 END) +
                          MAX(CASE WHEN create_data.auto_pay_category = 'CREATE' THEN create_data.failure ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS "CREATE SR",
          CONCAT(
              CAST(
                  ROUND(
                      MAX(CASE WHEN fe.auto_pay_category = '1st Execution' THEN fe.success ELSE NULL END) * 100.0 /
                      NULLIF(
                          MAX(CASE WHEN fe.auto_pay_category = '1st Execution' THEN fe.success ELSE 0 END) +
                          MAX(CASE WHEN fe.auto_pay_category = '1st Execution' THEN fe.failure ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS "1st Execution SR",
          CONCAT(
              CAST(
                  ROUND(
                      MAX(CASE WHEN ge.auto_pay_category = '>1 Executions' THEN ge.success ELSE NULL END) * 100.0 /
                      NULLIF(
                          MAX(CASE WHEN ge.auto_pay_category = '>1 Executions' THEN ge.success ELSE 0 END) +
                          MAX(CASE WHEN ge.auto_pay_category = '>1 Executions' THEN ge.failure ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS ">1 Executions SR",
          CONCAT(
              CAST(
                  ROUND(
                      MAX(CASE WHEN revoke_data.auto_pay_category = 'Revoke' THEN revoke_data.success ELSE NULL END) * 100.0 /
                      NULLIF(
                          MAX(CASE WHEN revoke_data.auto_pay_category = 'Revoke' THEN revoke_data.success ELSE 0 END) +
                          MAX(CASE WHEN revoke_data.auto_pay_category = 'Revoke' THEN revoke_data.failure ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS "Revoke SR",
          CONCAT(
              CAST(
                  ROUND(
                      MAX(CASE WHEN pdn_data.auto_pay_category = 'PDN' THEN pdn_data.success ELSE NULL END) * 100.0 /
                      NULLIF(
                          MAX(CASE WHEN pdn_data.auto_pay_category = 'PDN' THEN pdn_data.success ELSE 0 END) +
                          MAX(CASE WHEN pdn_data.auto_pay_category = 'PDN' THEN pdn_data.failure ELSE 0 END),
                          0
                      ),
                      2
                  ) AS VARCHAR
              ),
              '%'
          ) AS "PDN SR"
      FROM
          create_data
          FULL OUTER JOIN first_execution_data fe
              ON create_data.created_month = fe.created_month
          FULL OUTER JOIN greater_execution_data ge
              ON COALESCE(create_data.created_month, fe.created_month) = ge.created_month
          FULL OUTER JOIN revoke_data
              ON COALESCE(create_data.created_month, fe.created_month, ge.created_month) = revoke_data.created_month
          FULL OUTER JOIN pdn_data
              ON COALESCE(create_data.created_month, fe.created_month, ge.created_month, revoke_data.created_month) = pdn_data.created_month
      GROUP BY
          1
      ORDER BY
          1 DESC
       ;;
  }

  suggestions: no

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: created_month {
    type: time
    sql: ${TABLE}.created_month ;;
  }

  dimension: create_sr {
    type: string
    label: "CREATE SR"
    sql: ${TABLE}."CREATE SR" ;;
  }

  dimension: 1st_execution_sr {
    type: string
    label: "1st Execution SR"
    sql: ${TABLE}."1st Execution SR" ;;
  }

  dimension: 1_executions_sr {
    type: string
    label: ">1 Executions SR"
    sql: ${TABLE}.">1 Executions SR" ;;
  }

  dimension: revoke_sr {
    type: string
    label: "Revoke SR"
    sql: ${TABLE}."Revoke SR" ;;
  }

  dimension: pdn_sr {
    type: string
    label: "PDN SR"
    sql: ${TABLE}."PDN SR" ;;
  }

  set: detail {
    fields: [
      created_month_time,
      create_sr,
      1st_execution_sr,
      1_executions_sr,
      revoke_sr,
      pdn_sr
    ]
  }
}
