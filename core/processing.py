import pandas as pd
from itertools import product


def process_data(data, country_map):
    """
    Processes and merges data with the country mapping and reorders columns.

    :param data:            The input DataFrame.
    :param country_map:     A DataFrame with the country mapping.
    :return: A processed and ordered DataFrame
    """

    print(f'Processing Data: {len(data)} rows BEFORE merging the COUNTRIES mapping')

    data = pd.merge(left=data,
                    right=country_map,
                    how='left',
                    left_on=['kpi agency', 'branch'],
                    right_on=['kpi agency', 'branch'])

    print(f'Processing Data: {len(data)} rows AFTER merging the COUNTRIES mapping')

    column_order = ['agency code', 'kpi agency', 'fc code',
                    'branch', 'ceo region', 'continent split',
                    'regional director', 'department fc code', 'ftes', 'currency',
                    'month', 'year', 'date']

    data.reset_index(drop=True, inplace=True)

    return data[column_order]


def merge_grid_with_single_agency(single_agencies, agency_grid):
    """
    Merges single agency data into the agency grid.

    :param single_agencies: A dictionary with single agency data.
    :param agency_grid:     The main agency grid DataFrame.
    :return: The merged DataFrame.
    """

    data = agency_grid

    for key, val in single_agencies.items():

        data = pd.concat([data, val['fte']])
        print(f'{key.capitalize()}: data integrated to the Agency Grid set')

    data['ftes'] = data['ftes'].fillna(0.0)
    data.sort_values(by=['kpi agency', 'branch', 'department fc code'], axis=0, inplace=True, ignore_index=True)
    data.reset_index(drop=True, inplace=True)

    return data


def filter_agency_grid(data):
    """
    Select rows in the DataFrame where any cell has an empty value, NaN value, or the string "NOT ASSIGNED".

    :param data: The agency grid DataFrame.
    :return: Two DataFrames: one with avoided rows, and one with filtered rows
    """

    # Create a mask for rows with empty values, NaN values, or "NOT ASSIGNED"
    mask = data.map(lambda x: pd.isna(x) or x == "" or x == "not assigned" or x == "not included").any(axis=1)

    # Select rows where any condition is met
    avoided_data = data[mask].reset_index(drop=True)
    filtered_data = data[~mask].reset_index(drop=True)

    ordered_columns = ["date",
                       "kpi agency",
                       "branch",
                       "fc code",
                       "department fc code",
                       "currency",
                       "ftes"]

    avoided_data = avoided_data[ordered_columns]
    filtered_data = filtered_data[ordered_columns]

    print(f'Processing Data: {len(avoided_data)} rows AVOIDED in the Agency Grid')
    print(f'Processing Data: {len(filtered_data)} rows INCLUDED in the Agency Grid')

    return avoided_data, filtered_data


def get_fc_file(data):
    """
    Prepares a DataFrame for FC by transforming and structuring the data.

    :param data:    The input DataFrame.
    :return: A structured DataFrame for FC use
    """

    print(f'Processing file for FC: {len(data)} rows BEFORE FC processing')

    data['date'] = data['date'].dt.strftime('%Y.%m')

    # Split the data into the required parts
    df_right = data[['fc code', 'department fc code', 'ftes']].copy()
    df_left = data[['fc code', 'currency', 'date']].copy()

    # Create all possible combinations of 'fc code' and 'department fc code'
    all_combinations = pd.DataFrame(list(product(data['fc code'].unique(), data['department fc code'].unique())),
                                    columns=['fc code', 'department fc code'])

    # Merge all combinations with df_right to include all possible groups
    df_right = pd.merge(all_combinations, df_right, on=['fc code', 'department fc code'], how='left').fillna(0)

    # Group by 'fc code' and 'department fc code', summing 'ftes'
    df_right = df_right.groupby(['fc code', 'department fc code']).agg({'ftes': 'sum'}).reset_index()

    # Drop duplicates and sort df_left
    df_left.drop_duplicates(inplace=True, ignore_index=True)
    df_left.sort_values(by=['fc code'], axis=0, inplace=True, ignore_index=True)

    # Merge df_left and df_right
    data = pd.merge(left=df_left,
                    right=df_right,
                    how='left',
                    left_on='fc code',
                    right_on='fc code')

    # Rename columns
    data.rename(columns={'fc code': 'D_RU',
                         'currency': 'D_CU',
                         'date': 'D_DP',
                         'department fc code': 'D_AC',
                         'ftes': 'P_AMOUNT'}, inplace=True)

    # Add new columns
    data['D_CA'] = 'ACTS'
    data['D_AU'] = 'PACK01'
    data['D_FL'] = 'FTE01'

    # Select and reorder columns
    data = data[['D_CA', 'D_AU', 'D_FL', 'D_DP', 'D_RU', 'D_CU', 'D_AC', 'P_AMOUNT']].copy()

    # Convert all string data to uppercase
    data = data.map(lambda x: x.upper() if isinstance(x, str) else x)

    # Sort values
    data.sort_values(by=['D_RU', 'D_AC'], axis=0, inplace=True, ignore_index=True)
    data.reset_index(drop=True, inplace=True)

    print(f'Processing file for FC: {len(data)} rows AFTER FC processing')

    return data

