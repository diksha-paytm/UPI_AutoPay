view: creations_ptybl_and_non_ptybl_sr {
  derived_table: {
    sql: SELECT
          DATE(ti.created_on) AS created_date,

      -- Success Rate for Ptybl by Initiation Mode
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      THEN ti.umn
      END), 0) AS "ptybl_00_SR",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      THEN ti.umn
      END), 0) AS "ptybl_04_SR",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      THEN ti.umn
      END), 0) AS "ptybl_13_SR",

      -- Success Rate for Non-Ptybl by Initiation Mode
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      THEN ti.umn
      END), 0) AS "non_ptybl_00_SR",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      THEN ti.umn
      END), 0) AS "non_ptybl_04_SR",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      AND ti.status = 'SUCCESS'
      THEN ti.umn
      END) * 100.0 /
      NULLIF(COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      THEN ti.umn
      END), 0) AS "non_ptybl_13_SR"

      FROM
      team_product.looker_RM ti
      JOIN
      team_product.looker_txn_parti_RM tp
      ON
      ti.txn_id = tp.txn_id
      WHERE
      ti.type = 'CREATE'
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

  dimension: ptybl_00_sr {
    type: number
    sql: ${TABLE}.ptybl_00_SR ;;
  }

  dimension: ptybl_04_sr {
    type: number
    sql: ${TABLE}.ptybl_04_SR ;;
  }

  dimension: ptybl_13_sr {
    type: number
    sql: ${TABLE}.ptybl_13_SR ;;
  }

  dimension: non_ptybl_00_sr {
    type: number
    sql: ${TABLE}.non_ptybl_00_SR ;;
  }

  dimension: non_ptybl_04_sr {
    type: number
    sql: ${TABLE}.non_ptybl_04_SR ;;
  }

  dimension: non_ptybl_13_sr {
    type: number
    sql: ${TABLE}.non_ptybl_13_SR ;;
  }

  set: detail {
    fields: [
      created_date,
      ptybl_00_sr,
      ptybl_04_sr,
      ptybl_13_sr,
      non_ptybl_00_sr,
      non_ptybl_04_sr,
      non_ptybl_13_sr
    ]
  }
}
