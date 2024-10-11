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

# This should only have one dimension table
one_dimension: dict = {'SELECT': [{'field_name': 'bk_id_game'}],
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

base_table_with_and_agg_with_join: dict = {'SELECT': [{'field_name': 'year'}, ],
                                           'CALCULATION': [{'field_name': 'sales_rub', 'calculation': 'sum'},
                                                           {'field_name': 'pcs', 'calculation': 'sum'},
                                                           {'field_name': 'publisher_name', 'calculation': 'count'}],
                                           'WHERE': []}

base_table_with_and_agg_without_join: dict = {'SELECT': [{'field_name': 'year'}, {"field_name": "english"}],
                                              'CALCULATION': [{'field_name': 'game_name', 'calculation': 'count'}],
                                              'WHERE': []}

# This should only use base_sales table_no_where
base_table_with_no_join_wht_where: dict = {'SELECT': [{'field_name': 'year'},
                                                      {'field_name': 'pcs'}],
                                           'CALCULATION': [],
                                           'WHERE': [{'field_name': 'release_date',
                                                      'where': '>', 'condition': '2024-01-01'},
                                                     {'field_name': 'price',
                                                      'where': '>', 'condition': '1000'}
                                                     ]}

base_table_with_join_wht_where: dict = {'SELECT': [{'field_name': 'year'},
                                                   {'field_name': 'pcs'}],
                                        'CALCULATION': [],
                                        'WHERE': [{'field_name': 'release_date',
                                                   'where': '>', 'condition': '2024-01-01'},
                                                  {'field_name': 'price',
                                                   'where': '>', 'condition': '1000'},
                                                  {'field_name': 'game_name',
                                                   'where': '=', 'condition': 'The Best Game'}
                                                  ]}

# This should only use base_sales table
base_table_with_join_with_where: dict = {'SELECT': [{'field_name': 'year'},
                                                    {'field_name': 'pcs'}, ],
                                         'CALCULATION': [{'field_name': 'achievements', 'calculation': 'sum'},
                                                         {'field_name': 'pcs', 'calculation': 'sum'},
                                                         {'field_name': 'price', 'calculation': 'sum'}],
                                         'WHERE': [{'field_name': 'release_date',
                                                    'where': '>', 'condition': '2024-01-01'},
                                                   {'field_name': 'price',
                                                    'where': '>', 'condition': '1000'}]}

base_table_with_join_wth_where: dict = {'SELECT': [{'field_name': 'year'},
                                                   {'field_name': 'pcs'},
                                                   {'field_name': 'bk_id_game'}],
                                        'CALCULATION': [{'field_name': 'achievements', 'calculation': 'sum'},
                                                        {'field_name': 'pcs', 'calculation': 'sum'},
                                                        {'field_name': 'price', 'calculation': 'sum'}],
                                        'WHERE': [{'field_name': 'release_date',
                                                   'where': '>', 'condition': '2024-01-01'},
                                                  {'field_name': 'price',
                                                   'where': '>', 'condition': '1000'},
                                                  {'field_name': 'game_name',
                                                   'where': '=', 'condition': 'The Best Game'}
                                                  ]}

# This should only have one dimension table
one_dimension_count: dict = {'SELECT': [{'field_name': 'bk_id_game'}],
                             'CALCULATION': [{'field_name': 'bk_id_game', 'calculation': 'count'}],
                             'WHERE': []}

# This should only have one dimension table
one_dimension_count_where: dict = {'SELECT': [{'field_name': 'bk_id_game'}],
                                   'CALCULATION': [{'field_name': 'bk_id_game', 'calculation': 'count'}],
                                   'WHERE': [{'field_name': 'game_name', 'where': 'like', 'condition': 'a%'}]}

where_in_string: dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'game_name',
                                                   'where': 'IN', 'condition': ['Uno', 'Dos']}
                                                  ]}

where_in_number: dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'bk_id_game',
                                                   'where': 'IN', 'condition': ['12', '25']}
                                                  ]}

where_not_in_string : dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'game_name',
                                                   'where': 'NOT IN', 'condition': ['Uno', 'Dos']}
                                                  ]}

where_not_in_number: dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'bk_id_game',
                                                   'where': 'NOT IN', 'condition': ['12', '25']}
                                                  ]}

where_between_string : dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'game_name',
                                                   'where': 'BETWEEN', 'condition': ['Uno', 'Dos']}
                                                  ]}

where_between_numbers : dict = {'SELECT': [], 'CALCULATION': [],
                                        'WHERE': [{'field_name': 'bk_id_game',
                                                   'where': 'BETWEEN', 'condition': [1, 1000]}
                                                  ]}

