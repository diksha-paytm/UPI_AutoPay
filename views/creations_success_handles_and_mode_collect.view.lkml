view: creations_success_handles_and_mode_collect {
  derived_table: {
    sql: WITH handle_data AS (
          SELECT
              DATE(ti.created_on) AS created_date,
              LOWER(SUBSTRING(ti.umn FROM POSITION('@' IN ti.umn) + 1)) AS handle,
              REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
              COUNT(DISTINCT CASE WHEN ti.status = 'SUCCESS' THEN ti.umn ELSE NULL END) AS success
          FROM hive.switch.txn_info_snapshot_v3 ti
          WHERE
              ti.business_type = 'MANDATE'
              AND JSON_QUERY(ti.extended_info, 'strict$.purpose') = '"14"'
              AND ti.dl_last_updated >= DATE_ADD('day', -50, CURRENT_DATE)
              AND ti.created_on >= CAST(DATE_ADD('day', -50, CURRENT_DATE) AS TIMESTAMP)
              AND ti.created_on < CAST(CURRENT_DATE AS TIMESTAMP)
              AND ti.type = 'CREATE'
              AND ti.status = 'SUCCESS'
              AND LOWER(SUBSTRING(ti.umn, POSITION('@' IN ti.umn) + 1)) IN ('ptaxis', 'pthdfc', 'ptsbi', 'ptyes')
              AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') IN ('00')
          GROUP BY 1, 2, 3
      )
      SELECT
          created_date,
          -- PTAXIS
          MAX(CASE WHEN handle = 'ptaxis' AND initiation_mode = '00' THEN success ELSE 0 END) AS "ptaxis Collect",

      -- PTHDFC
      MAX(CASE WHEN handle = 'pthdfc' AND initiation_mode = '00' THEN success ELSE 0 END) AS "pthdfc Collect",

      -- PTSBI
      MAX(CASE WHEN handle = 'ptsbi' AND initiation_mode = '00' THEN success ELSE 0 END) AS "ptsbi Collect",

      -- PTYES
      MAX(CASE WHEN handle = 'ptyes' AND initiation_mode = '00' THEN success ELSE 0 END) AS "ptyes Collect"
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

  dimension: ptaxis_collect {
    type: number
    label: "ptaxis Collect"
    sql: ${TABLE}."ptaxis Collect" ;;
  }

  dimension: pthdfc_collect {
    type: number
    label: "pthdfc Collect"
    sql: ${TABLE}."pthdfc Collect" ;;
  }

  dimension: ptsbi_collect {
    type: number
    label: "ptsbi Collect"
    sql: ${TABLE}."ptsbi Collect" ;;
  }

  dimension: ptyes_collect {
    type: number
    label: "ptyes Collect"
    sql: ${TABLE}."ptyes Collect" ;;
  }

  set: detail {
    fields: [created_date, ptaxis_collect, pthdfc_collect, ptsbi_collect, ptyes_collect]
  }
}
