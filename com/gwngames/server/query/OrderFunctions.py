from com.gwngames.server.query.QueryBuilder import QueryBuilder


def handle_order_by(qb: QueryBuilder, order_column: str, order_type: str):
    if order_column == "Frequent Journal Rank":
        qb.order_by(f"""
                                        CASE fj.freq_journal_rank
                                            WHEN 'Q1' THEN 1
                                            WHEN 'Q2' THEN 2
                                            WHEN 'Q3' THEN 3
                                            WHEN 'Q4' THEN 4
                                            ELSE 5
                                          END {order_type},
                                        \"{order_column}\"
                                        """, True if order_type == "ASC" else False)
    elif order_column == "Journal Rank":
        qb.order_by(f"""
                            CASE j.q_rank
                                WHEN 'Q1' THEN 1
                                WHEN 'Q2' THEN 2
                                WHEN 'Q3' THEN 3
                                WHEN 'Q4' THEN 4
                                ELSE 5
                              END {order_type},
                            \"{order_column}\"
                            """, True if order_type == "ASC" else False)
    elif order_column == "Frequent Conf. Rank":
        qb.order_by(
            f"""
            CASE fc.freq_conf_rank
                WHEN 'A*' THEN 1
                    WHEN 'A' THEN 2
                    WHEN 'B' THEN 3
                    WHEN 'C' THEN 4
                    ELSE 5
                  END {order_type},
                \"{order_column}\"
                """, True if order_type == "ASC" else False
        )
    elif order_column == "Conference Rank":
        qb.order_by(f"""
                CASE c.rank
                    WHEN 'A*' THEN 1
                    WHEN 'A' THEN 2
                    WHEN 'B' THEN 3
                    WHEN 'C' THEN 4
                    ELSE 5
                  END {order_type},
                \"{order_column}\"
                """, True if order_type == "ASC" else False)
    else:
        qb.order_by(f"\"{order_column}\"", True if order_type == "ASC" else False)