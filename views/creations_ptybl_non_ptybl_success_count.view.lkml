view: creations_ptybl_non_ptybl_success_count {
  derived_table: {
    sql: SELECT
          date(created_on) AS created_date,

      -- For PTYBL
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      THEN ti.umn END) AS "ptybl_00",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      THEN ti.umn END) AS "ptybl_04",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      THEN ti.umn END) AS "ptybl_13",

      -- For NON-PTYBL
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '00'
      THEN ti.umn END) AS "non_ptybl_00",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '04'
      THEN ti.umn END) AS "non_ptybl_04",

      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') = '13'
      THEN ti.umn END) AS "non_ptybl_13"

      FROM team_product.looker_RM ti
      JOIN team_product.looker_txn_parti_RM tp
      ON ti.txn_id = tp.txn_id
      WHERE
      ti.type = 'CREATE'
      AND ti.status = 'SUCCESS'
      GROUP BY 1
      ORDER BY 1 DESC
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

  dimension: ptybl_00 {
    type: number
    sql: ${TABLE}.ptybl_00 ;;
  }

  dimension: ptybl_04 {
    type: number
    sql: ${TABLE}.ptybl_04 ;;
  }

  dimension: ptybl_13 {
    type: number
    sql: ${TABLE}.ptybl_13 ;;
  }

  dimension: non_ptybl_00 {
    type: number
    sql: ${TABLE}.non_ptybl_00 ;;
  }

  dimension: non_ptybl_04 {
    type: number
    sql: ${TABLE}.non_ptybl_04 ;;
  }

  dimension: non_ptybl_13 {
    type: number
    sql: ${TABLE}.non_ptybl_13 ;;
  }

  set: detail {
    fields: [
      created_date,
      ptybl_00,
      ptybl_04,
      ptybl_13,
      non_ptybl_00,
      non_ptybl_04,
      non_ptybl_13
    ]
  }
}
