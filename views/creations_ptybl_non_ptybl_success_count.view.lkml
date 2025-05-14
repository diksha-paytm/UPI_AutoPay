view: creations_ptybl_non_ptybl_success_count {
  derived_table: {
    sql: SELECT date(created_on) as created_date,
      REPLACE(JSON_QUERY(ti.extended_info, 'strict $.initiationMode'), '"', '') AS initiation_mode,
      count(distinct case when LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) = 'ptybl' then ti.umn else null end) as "ptybl",
      count(distinct case when LOWER(SUBSTRING(tp.vpa FROM POSITION('@' IN tp.vpa) + 1)) != 'ptybl' then ti.umn else null end) as "non-ptybl"
      FROM team_product.looker_RM ti
      join team_product.looker_txn_parti_RM tp
      on ti.txn_id=tp.txn_id
                WHERE
                    ti.type = 'CREATE'
                    AND ti.status='SUCCESS'
      group by 1,2
      order by 1 desc,2
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

  dimension: ptybl {
    type: number
    sql: ${TABLE}.ptybl ;;
  }

  dimension: nonptybl {
    type: number
    sql: ${TABLE}."non-ptybl" ;;
  }

  set: detail {
    fields: [created_date, initiation_mode, ptybl, nonptybl]
  }
}
