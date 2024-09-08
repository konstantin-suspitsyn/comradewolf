# This should only use base_sales table_no_where
base_table_with_join_no_where_no_calc: dict = {'SELECT': [{'field_name': 'year'},
                                                          {'field_name': 'pcs'}],
                                               'CALCULATION': [],
                                               'WHERE': []}
# This should only use base_sales table
base_table_with_join_no_where: dict = {'SELECT': [{'field_name': 'year'},
                                                  {'field_name': 'pcs'},
                                                  {'field_name': 'bk_id_game'}],
                                       'CALCULATION': [{'field_name': 'achievements', 'calculation': 'sum'},
                                                       {'field_name': 'pcs', 'calculation': 'sum'},
                                                       {'field_name': 'price', 'calculation': 'sum'}],
                                       'WHERE': []}

# This should only use base_sales table
group_by_read_no_where: dict = {'SELECT': [{'field_name': 'year'},
                                           {'field_name': 'yearmonth'}, ],
                                'CALCULATION': [{'field_name': 'price', 'calculation': 'avg'},
                                                {'field_name': 'pcs', 'calculation': 'sum'}, ],
                                'WHERE': []}

# This should only use base_sales table
group_by_also_in_agg: dict = {'SELECT': [{'field_name': 'year'}, ],
                              'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                              {'field_name': 'pcs', 'calculation': 'sum'}, ],
                              'WHERE': []}

# This should only use base_sales table
one_agg_value: dict = {'SELECT': [],
                       'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'}],
                       'WHERE': []}

# This should only use base_sales table
one_dimension: dict = {'SELECT': [{'field_name': 'bk_game_id'}],
                       'CALCULATION': [],
                       'WHERE': []}

base_table_with_join_no_gb: dict = {'SELECT': [{'field_name': 'year'},
                                               {'field_name': 'pcs'},
                                               {'field_name': 'bk_id_game'},
                                               {"field_name": "publisher_name"}],
                                    'CALCULATION': [],
                                    'WHERE': []}

base_table_with_and_agg: dict = {'SELECT': [{'field_name': 'year'},
                                            {"field_name": "publisher_name"}, ],
                                 'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                                 {'field_name': 'pcs', 'calculation': 'sum'}],
                                 'WHERE': []}

base_table_with_and_agg_with_join: dict = {'SELECT': [{'field_name': 'year'},],
                                           'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                                           {'field_name': 'pcs', 'calculation': 'sum'},
                                                           {'field_name': 'publisher_name', 'calculation': 'count'}],
                                           'WHERE': []}
