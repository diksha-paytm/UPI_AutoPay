view: creations_handle_mode_sr {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) AS handle,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              ROUND(
    COUNT(DISTINCT CASE
        WHEN status = 'SUCCESS'
        THEN umn
        ELSE NULL
      END
    ) * 100.0 /
   NULLIF(COUNT(DISTINCT umn), 0), 2
  ) AS sr
  FROM team_product.looker_RM ti
          WHERE
              ti.type = 'CREATE'
              AND LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes')
          GROUP BY 1, 2, 3
      )
      SELECT
          created_date,

      -- PTAXIS
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '00' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptaxis_Collect_SR",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '04' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptaxis_Intent_SR",
      MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '13' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptaxis_QR_SR",

      -- PTHDFC
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '00' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "pthdfc_Collect_SR",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '04' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "pthdfc_Intent_SR",
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '13' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "pthdfc_QR_SR",

      -- PTSBI
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '00' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptsbi_Collect_SR",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '04' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptsbi_Intent_SR",
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '13' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptsbi_QR_SR",

      -- PTYES
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '00' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptyes_Collect_SR",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '04' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptyes_Intent_SR",
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '13' THEN CONCAT(CAST(sr AS VARCHAR), '%') ELSE NULL END) AS "ptyes_QR_SR",

      -- Average SR for each mode
      CONCAT(
      CAST(ROUND(
      SUM(CASE WHEN initiation_mode = '00' THEN sr ELSE NULL END) / NULLIF(COUNT(CASE WHEN initiation_mode = '00' THEN sr ELSE NULL END), 0),
      2) AS VARCHAR), '%'
      ) AS "Average_Collect_SR",

      CONCAT(
      CAST(ROUND(
      SUM(CASE WHEN initiation_mode = '04' THEN sr ELSE NULL END) / NULLIF(COUNT(CASE WHEN initiation_mode = '04' THEN sr ELSE NULL END), 0),
      2) AS VARCHAR), '%'
      ) AS "Average_Intent_SR",

      CONCAT(
      CAST(ROUND(
      SUM(CASE WHEN initiation_mode = '13' THEN sr ELSE NULL END) / NULLIF(COUNT(CASE WHEN initiation_mode = '13' THEN sr ELSE NULL END), 0),
      2) AS VARCHAR), '%'
      ) AS "Average_QR_SR"

      FROM handle_data
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

  dimension: average_collect_sr {
    type: string
    sql: ${TABLE}.Average_Collect_SR ;;
  }

  dimension: average_intent_sr {
    type: string
    sql: ${TABLE}.Average_Intent_SR ;;
  }

  dimension: average_qr_sr {
    type: string
    sql: ${TABLE}.Average_QR_SR ;;
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
      ptyes_qr_sr,
      average_collect_sr,
      average_intent_sr,
      average_qr_sr
    ]
  }
}
