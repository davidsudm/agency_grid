import numpy as np
import pandas as pd
import difflib


def get_data_columns():
    """
    Returns a predefined list of column names for KPI data.

    :return:    A list of column names to be used in the data processing workflow.
    """

    # Reorder columns
    columns = ['kpi agency',
               'branch',
               'ceo region',
               'regional director',
               'department fc code',
               'fte type',
               'n fte',
               'month',
               'year',
               'date',
               ]

    return columns


def clean_cell(x):
    """
    Cleans a given cell by removing asterisks and extra spaces.

    :param x:   The input string or cell value to be cleaned.
    :return: A cleaned version of the input, free from asterisks and extra spaces.
    """

    x = str(x).replace('*', '')
    x = ' '.join(x.split())

    return x


def clean_up_data(data):
    """
    Cleans up the DataFrame by:
    - Stripping and lowercasing column names
    - Converting all data to strings
    - Removing asterisks (*)
    - Removing multiple spaces but not single spaces
    - Removing leading and trailing spaces
    - Converting 'cognos code' column to integers

    :param data:    A pandas DataFrame that contains the input data.
    :return: A cleaned DataFrame with the applied transformations.
    """

    # Clean column names
    data.columns = data.columns.str.strip().str.lower().str.replace(r'\s\s+', ' ', regex=True)
    # Convert all data to strings and apply cleaning operations
    for col in data.columns:
        data[col] = data[col].apply(lambda x: clean_cell(x).lower() if isinstance(x, str) else clean_cell(x))

    return data


def transform_mapping(data, key):
    """
    Transforms data based on a given key to either clean or rename columns, or adjust the data types of certain fields.

    :param data:    The input DataFrame to be transformed.
    :param key:     The key representing the type of transformation, such as 'countries' or 'departments'.
    :return: The transformed DataFrame.
    """

    if key == 'countries':

        data['agency code'] = data['agency code'].astype(str)

    elif key == 'departments':

        data.rename(columns={'kpi department': 'department'}, inplace=True)
        data['department fc code'] = data['department fc code'].astype(str)
        data['department'] = data['department'].astype(str)

    else:
        print(f"Issue in the mapping file, sheet {key}, while transforming data")

    return data


def transform_data(data, country_map):
    """
    Transforms a DataFrame by renaming columns, adjusting data types, and merging with the country_map DataFrame.

    :param data:            The input DataFrame for transformation.
    :param country_map:     A DataFrame containing the country mapping information for merging.
    :return: The transformed DataFrame after merging and restructuring.
    """

    data.rename(columns={'kpi year month': 'date'}, inplace=True)

    data['ftes'] = data['ftes'].astype(float)
    data['department fc code'] = data['department fc code'].apply(lambda x: f'x{x}0' if pd.notna(x) else x)

    # Convert 'kpi_agency' column to datetime format
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m')
    # Convert to Period with monthly frequency
    data['period'] = data['date'].dt.to_period('M')
    # Extract year and month
    data['year'] = data['period'].dt.year
    data['month'] = data['period'].dt.month

    df_merged = pd.merge(left=data, right=country_map,
                         left_on=['kpi agency', 'branch'],
                         right_on=['kpi agency', 'branch'],
                         how='left')

    if len(data) == len(df_merged):
        print(f'Data were not lost in the transformation, still {len(data)} rows')
    else:
        print(f'Raw data rows: {len(data)}')
        print(f'Processed data rows: {len(df_merged)}')
        print('Data decreased after processing')
        exit()

    return data


def transform_single_data(data, agency, country_map, department_map, date):
    """
    Processes data for a single agency, transforming it to match the general structure by applying several cleaning,
    renaming, and formatting steps.

    :param agency:          The DataFrame containing the agency data.
    :param data:            The name of the agency.
    :param country_map:     DataFrame with the country mapping.
    :param department_map:  DataFrame with the department mapping.
    :param date:            A dictionary with date information.
    :return: The transformed DataFrame ready for further integration.
    """

    data.rename(columns={data.columns[0]: 'department name'}, inplace=True)
    data.rename(columns={data.columns[1]: 'fte type'}, inplace=True)

    # Convert common string NaNs to actual NaNs
    nan_values = ['nan', 'none', 'null', '', 'n/a', 'na']
    data = data.map(lambda x: np.nan if str(x).strip().lower() in nan_values else x)

    # Look for branches of the entity (agency)
    target_column = "total for all branches"
    target_colum_index = data.columns.get_loc(target_column)
    data = data.iloc[:, :target_colum_index]

    target_row = 'service/documentation center'
    target_row_index = data[data['department name'] == target_row].index[0]

    data_fte = data.loc[:target_row_index-1, :].reset_index(drop=True)
    data_fte.insert(loc=0, column='department fc code', value=pd.NA)

    data_grand_total = data.loc[target_row_index:, :].reset_index(drop=True)

    # Fill the department fc code: Stage 1
    for name in data_fte['department name'].unique():
        matches = difflib.get_close_matches(name, department_map['department'], n=1, cutoff=0.6)
        match = matches[0]
        result = department_map[department_map['department'] == match]['department fc code'].values[0]
        data_fte.loc[data_fte['department name'] == name, 'department fc code'] = result

    data_grand_total.insert(loc=0, column='department fc code', value=pd.NA)

    # TODO: This part is unluckily hardcoded but if should be fixed once the labels in fte type is amended
    condition = data_grand_total['department name'] == target_row
    data_grand_total.loc[condition, 'department fc code'] = 'x9704000'
    for (code, meaning) in [('x9704100', "nb. of trainees (not reported in the fte's)"),
                            ('x9704200', 'nb. with temporary contracts'),
                            ('x9704300', 'nb. of fte in long leave commitment'),]:
        condition = data_grand_total['fte type'] == meaning
        data_grand_total.loc[condition, 'department fc code'] = code

    # TODO: this is linked to the upper TODO block
    data_fte = data_fte.loc[data_fte['fte type'] == 'nb of ftes', :]
    data_grand_total = data_grand_total.loc[~data_grand_total['department fc code'].isna(), :]
    data = pd.concat([data_fte, data_grand_total])

    data_temp = []
    for branch in data.columns[3:]:
        df_temp = data[['department fc code', branch]].copy()
        df_temp.rename(columns={branch: 'ftes'}, inplace=True)
        df_temp.insert(loc=0, column='branch', value=branch)
        data_temp.append(df_temp)
    data = pd.concat(data_temp)

    data['ftes'] = data['ftes'].astype(float)
    data['ftes'] = data['ftes'].fillna(0)

    data.insert(loc=0, column='kpi agency', value=agency)
    data.insert(loc=3, column='date', value=date['date'])
    data.insert(loc=5, column='period', value=data['date'].dt.to_period('M'))
    data.insert(loc=6, column='year', value=data['period'].dt.year)
    data.insert(loc=7, column='month', value=data['period'].dt.month)

    return data
