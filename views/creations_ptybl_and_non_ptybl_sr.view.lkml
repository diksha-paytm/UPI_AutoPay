view: creations_ptybl_and_non_ptybl_sr {
  derived_table: {
    sql: SELECT
          DATE(ti.created_on) AS created_date,
          REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,

      -- Calculate Success Rate for Ptybl with Zero Division Check and add %
      CASE
      WHEN COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      THEN ti.umn
      END) = 0
      THEN '0%'
      ELSE CONCAT(
      CAST(
      ROUND(
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      AND ti.status = 'SUCCESS' THEN ti.umn
      END) * 100.0 /
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl'
      THEN ti.umn
      END),
      2
      ) AS VARCHAR
      ), '%'
      )
      END AS "ptybl_SR",

      -- Calculate Success Rate for Non-Ptybl with Zero Division Check and add %
      CASE
      WHEN COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      THEN ti.umn
      END) = 0
      THEN '0%'
      ELSE CONCAT(
      CAST(
      ROUND(
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      AND ti.status = 'SUCCESS' THEN ti.umn
      END) * 100.0 /
      COUNT(DISTINCT CASE
      WHEN LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl'
      THEN ti.umn
      END),
      2
      ) AS VARCHAR
      ), '%'
      )
      END AS "non_ptybl_SR"

      FROM
      team_product.looker_RM ti
      JOIN
      team_product.looker_txn_parti_RM tp
      ON
      ti.txn_id = tp.txn_id
      WHERE
      ti.type = 'CREATE'
      GROUP BY
      1, 2
      ORDER BY
      1 DESC, 2
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

  dimension: initiation_mode {
    type: string
    sql: ${TABLE}.initiation_mode ;;
  }

  dimension: ptybl_sr {
    type: string
    sql: ${TABLE}.ptybl_SR ;;
  }

  dimension: non_ptybl_sr {
    type: string
    sql: ${TABLE}.non_ptybl_SR ;;
  }

  set: detail {
    fields: [created_date, initiation_mode, ptybl_sr, non_ptybl_sr]
  }
}
