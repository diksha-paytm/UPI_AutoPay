view: overall_count {
  derived_table: {
    sql: WITH create_data AS (
          SELECT
              DATE(created_on) AS created_date,
              'CREATE' AS auto_pay_category,
              COUNT(
                  CASE
                      WHEN status = 'SUCCESS' THEN txn_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  CASE
                      WHEN status = 'FAILURE' THEN txn_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'CREATE'
              AND SUBSTRING(
    umn
    FROM
      POSITION('@' IN umn) + 1
  )!='paytm'
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      first_execution_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
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
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'COLLECT'
              AND CAST(REPLACE(JSON_QUERY(extended_info, 'strict $.MANDATE_EXECUTION_NUMBER'), '"', '') AS INTEGER) = 1
              AND ti.status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      greater_execution_data AS (
          SELECT
              DATE(created_on) AS created_date,
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
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'COLLECT'
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      payer_initiated_revoke AS (
          SELECT
              DATE(created_on) AS created_date,
              'Payer_initiated_Revokes' AS auto_pay_category,
              COUNT(
                  CASE
                      WHEN status = 'SUCCESS' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS success,
              COUNT(
                  DISTINCT CASE
                      WHEN status = 'FAILURE' THEN ti.txn_id
                      ELSE NULL
                  END
              ) AS failure
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              business_type = 'MANDATE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND first_phase != 'REQMANDATECONFIRMATION-REVOKE'
              AND dl_last_updated IS NOT NULL
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'REVOKE'
              AND status IN ('FAILURE', 'SUCCESS')
          GROUP BY
              1
      ),
      payee_initiated_revoke AS (
          SELECT
              DATE(created_on) AS created_date,
              'Payee_initiated_Revokes' AS auto_pay_category,
              COUNT(*) AS success
          FROM
              hive.switch.txn_info_snapshot_v3 ti
          WHERE
              business_type = 'MANDATE'
              AND first_phase = 'REQMANDATECONFIRMATION-REVOKE'
              AND JSON_QUERY(extended_info, 'strict$.purpose') = '"14"'
              AND dl_last_updated IS NOT NULL
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND type = 'REVOKE'
              AND status = 'SUCCESS'
          GROUP BY
              1
      ),
      pdn_data AS (
          SELECT
              DATE(created_on) AS created_date,
              'PDN' AS auto_pay_category,
              COUNT(*) AS success
          FROM
              hive.switch.financial_notification_snapshot_v3 fn
          WHERE
              dl_last_updated IS NOT NULL
              AND created_on >= CAST(DATE_ADD('day', -100, CURRENT_DATE) AS TIMESTAMP)
              AND created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND status = 'SUCCESS'
          GROUP BY
              1
      )
      SELECT
          COALESCE(create_data.created_date, fe.created_date, ge.created_date, pe.created_date, pe1.created_date, pdn.created_date) AS created_date,
          MAX(CASE WHEN create_data.auto_pay_category = 'CREATE' THEN create_data.success ELSE NULL END) AS "CREATE Success",
          MAX(CASE WHEN create_data.auto_pay_category = 'CREATE' THEN create_data.failure ELSE NULL END) AS "CREATE Failure",
          MAX(CASE WHEN fe.auto_pay_category = '1st Execution' THEN fe.success ELSE NULL END) AS "1st Execution Success",
          MAX(CASE WHEN fe.auto_pay_category = '1st Execution' THEN fe.failure ELSE NULL END) AS "1st Execution Failure",
          MAX(CASE WHEN ge.auto_pay_category = '>1 Executions' THEN ge.success ELSE NULL END) AS ">1 Executions Success",
          MAX(CASE WHEN ge.auto_pay_category = '>1 Executions' THEN ge.failure ELSE NULL END) AS ">1 Executions Failure",
          MAX(CASE WHEN pe.auto_pay_category = 'Payer_initiated_Revokes' THEN pe.success ELSE NULL END) AS "Payer revoke Success",
          MAX(CASE WHEN pe.auto_pay_category = 'Payer_initiated_Revokes' THEN pe.failure ELSE NULL END) AS "Payer revoke Failure",
          MAX(CASE WHEN pe1.auto_pay_category = 'Payee_initiated_Revokes' THEN pe1.success ELSE NULL END) AS "Payee revoke Success",
          MAX(CASE WHEN pdn.auto_pay_category = 'PDN' THEN pdn.success ELSE NULL END) AS "PDN Success"
      FROM
          create_data
          FULL OUTER JOIN first_execution_data fe
              ON create_data.created_date = fe.created_date
          FULL OUTER JOIN greater_execution_data ge
              ON COALESCE(create_data.created_date, fe.created_date) = ge.created_date
          FULL OUTER JOIN payer_initiated_revoke pe
              ON COALESCE(create_data.created_date, fe.created_date, ge.created_date) = pe.created_date
          FULL OUTER JOIN payee_initiated_revoke pe1
              ON COALESCE(create_data.created_date, fe.created_date, ge.created_date, pe.created_date) = pe1.created_date
          FULL OUTER JOIN pdn_data pdn
              ON COALESCE(create_data.created_date, fe.created_date, ge.created_date, pe.created_date, pe1.created_date) = pdn.created_date
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

  dimension: created_date {
    type: date
    sql: ${TABLE}.created_date ;;
  }

  dimension: create_success {
    type: number
    label: "CREATE Success"
    sql: ${TABLE}."CREATE Success" ;;
  }

  dimension: create_failure {
    type: number
    label: "CREATE Failure"
    sql: ${TABLE}."CREATE Failure" ;;
  }

  dimension: 1st_execution_success {
    type: number
    label: "1st Execution Success"
    sql: ${TABLE}."1st Execution Success" ;;
  }

  dimension: 1st_execution_failure {
    type: number
    label: "1st Execution Failure"
    sql: ${TABLE}."1st Execution Failure" ;;
  }

  dimension: 1_executions_success {
    type: number
    label: ">1 Executions Success"
    sql: ${TABLE}.">1 Executions Success" ;;
  }

  dimension: 1_executions_failure {
    type: number
    label: ">1 Executions Failure"
    sql: ${TABLE}.">1 Executions Failure" ;;
  }

  dimension: payer_revoke_success {
    type: number
    label: "Payer revoke Success"
    sql: ${TABLE}."Payer revoke Success" ;;
  }

  dimension: payer_revoke_failure {
    type: number
    label: "Payer revoke Failure"
    sql: ${TABLE}."Payer revoke Failure" ;;
  }

  dimension: payee_revoke_success {
    type: number
    label: "Payee revoke Success"
    sql: ${TABLE}."Payee revoke Success" ;;
  }

  dimension: pdn_success {
    type: number
    label: "PDN Success"
    sql: ${TABLE}."PDN Success" ;;
  }

  set: detail {
    fields: [
      created_date,
      create_success,
      create_failure,
      1st_execution_success,
      1st_execution_failure,
      1_executions_success,
      1_executions_failure,
      payer_revoke_success,
      payer_revoke_failure,
      payee_revoke_success,
      pdn_success
    ]
  }
}
