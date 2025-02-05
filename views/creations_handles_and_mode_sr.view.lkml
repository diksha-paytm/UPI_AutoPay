view: creations_handles_and_mode_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) AS handle,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success,
              COUNT(DISTINCT CASE WHEN ti.status = 'FAILURE' THEN ti.umn ELSE NULL END) AS failure
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status IN ('FAILURE', 'SUCCESS')
              AND LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes')
          GROUP BY 1, 2, 3
      ),
      pivoted_data AS (
          SELECT
              created_date,
              handle,
              initiation_mode,
              CONCAT(
                  CAST(ROUND(success * 100.0 / NULLIF(success + failure, 0), 2) AS VARCHAR),
                  '%'
              ) AS sr
          FROM handle_data
          WHERE initiation_mode IN ('00', '04', '13')
      )
      SELECT
          created_date,

      -- PTAXIS
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "ptaxis_Collect_SR",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "ptaxis_Intent_SR",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "ptaxis_QR_SR",

      -- PTHDFC
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "pthdfc_Collect_SR",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "pthdfc_Intent_SR",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "pthdfc_QR_SR",

      -- PTSBI
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "ptsbi_Collect_SR",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "ptsbi_Intent_SR",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "ptsbi_QR_SR",

      -- PTYES
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '00' THEN sr ELSE NULL END) AS "ptyes_Collect_SR",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '04' THEN sr ELSE NULL END) AS "ptyes_Intent_SR",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '13' THEN sr ELSE NULL END) AS "ptyes_QR_SR"

      FROM pivoted_data
      GROUP BY created_date
      ORDER BY created_date DESC
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

  dimension: ptaxis_collect_sr {
    type: string
    sql: ${TABLE}.ptaxis_Collect_SR ;;
  }

  dimension: ptaxis_intent_sr {
    type: string
    sql: ${TABLE}.ptaxis_Intent_SR ;;
  }

  dimension: ptaxis_qr_sr {
    type: string
    sql: ${TABLE}.ptaxis_QR_SR ;;
  }

  dimension: pthdfc_collect_sr {
    type: string
    sql: ${TABLE}.pthdfc_Collect_SR ;;
  }

  dimension: pthdfc_intent_sr {
    type: string
    sql: ${TABLE}.pthdfc_Intent_SR ;;
  }

  dimension: pthdfc_qr_sr {
    type: string
    sql: ${TABLE}.pthdfc_QR_SR ;;
  }

  dimension: ptsbi_collect_sr {
    type: string
    sql: ${TABLE}.ptsbi_Collect_SR ;;
  }

  dimension: ptsbi_intent_sr {
    type: string
    sql: ${TABLE}.ptsbi_Intent_SR ;;
  }

  dimension: ptsbi_qr_sr {
    type: string
    sql: ${TABLE}.ptsbi_QR_SR ;;
  }

  dimension: ptyes_collect_sr {
    type: string
    sql: ${TABLE}.ptyes_Collect_SR ;;
  }

  dimension: ptyes_intent_sr {
    type: string
    sql: ${TABLE}.ptyes_Intent_SR ;;
  }

  dimension: ptyes_qr_sr {
    type: string
    sql: ${TABLE}.ptyes_QR_SR ;;
  }

  set: detail {
    fields: [
      created_date,
      ptaxis_collect_sr,
      ptaxis_intent_sr,
      ptaxis_qr_sr,
      pthdfc_collect_sr,
      pthdfc_intent_sr,
      pthdfc_qr_sr,
      ptsbi_collect_sr,
      ptsbi_intent_sr,
      ptsbi_qr_sr,
      ptyes_collect_sr,
      ptyes_intent_sr,
      ptyes_qr_sr
    ]
  }
}
