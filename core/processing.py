import pandas as pd


def process_data(data, departments_map, country_map):
    """

    :param data:
    :param departments_map:
    :param country_map:
    :return:
    """

    not_standard_description_list = departments_map.tail(4)['department'].values

    for item in not_standard_description_list:
        condition = data['fte type'] == item
        data.loc[condition, 'department'] = data.loc[condition, 'fte type']

    print(len(data))
    data_merged = pd.merge(left=data,
                           right=departments_map,
                           how='left',
                           left_on='department',
                           right_on='department')
    print(len(data_merged))
    data_merged = pd.merge(left=data_merged,
                           right=country_map,
                           how='left',
                           left_on=['kpi agency', 'branch'],
                           right_on=['kpi agency', 'branch'])
    print(len(data_merged))

    print(data_merged.info())
    print(data_merged.describe())

    return data_merged


def merge_grid_with_single_agency(single_agencies, agency_grid):
    """

    :param single_agencies:
    :param agency_grid:
    :return:
    """

    data = agency_grid

    for key, val in single_agencies.items():

        data = pd.concat([data, val['fte']])
        print(f'{key.capitalize()}: data integrated to the Agency Grid set')

    return data


def get_fc_file(data):
    """

    :param data:
    :return:
    """

    selected_columns = ['date', 'fc code', 'currency', 'department', 'n fte']

    data['date'] = data['date'].dt.strftime('%Y.%m')

    df_for_fc = data[selected_columns].copy()
    column_names = ['D_DP', 'D_RU', 'D_CU', 'D_AC', 'P_AMOUNT']
    df_for_fc.columns = column_names

    df_for_fc['D_CA'] = 'ACTS'
    df_for_fc['D_AU'] = 'PACK01'
    df_for_fc['D_FL'] = 'FTE01'

    df_for_fc = df_for_fc.map(lambda x: x.upper() if isinstance(x, str) else x)
    df_for_fc = df_for_fc[['D_CA', 'D_AU', 'D_FL', 'D_DP', 'D_RU', 'D_CU', 'D_AC', 'P_AMOUNT']]

    return df_for_fc
