import numpy as np
import pandas as pd


def clean_up_data(data):
    """
    Cleans up the DataFrame by:
    - Stripping and lowercasing column names
    - Converting all data to strings
    - Removing asterisks (*)
    - Removing multiple spaces but not single spaces
    - Removing leading and trailing spaces

    :param data: Input DataFrame
    :return: Cleaned DataFrame
    """

    # Clean column names
    data.columns = data.columns.str.strip().str.lower().str.replace(r'\s\s+', ' ', regex=True)

    # Convert all data to strings and apply cleaning operations
    data = data.map(lambda x: str(x).lower() if isinstance(x, str) else str(x))
    data = data.map(lambda x: x.replace('*', ''))
    # Removes multiple spaces and strips leading/trailing spaces
    data = data.map(lambda x: ' '.join(x.split()))

    return data


def amending_wrong_inputs(data):
    """

    :param data:
    :return:
    """

    exceptions_raw = [
        (' / ', '/'),
        ('nb', 'no'),
        ('.', ''),
        ('transshipment', 'transhipment')
    ]

    exceptions = [(a.lower(), b.lower()) for a, b in exceptions_raw]

    # Iterate over all columns in the DataFrame
    columns_to_exclude = ['cognos code', 'current total # employees', 'agency code']
    # Get the columns of object dtype
    object_columns = data.select_dtypes(include='object').columns
    # Filter out the specified columns
    filtered_columns = [col for col in object_columns if col not in columns_to_exclude]

    for col in filtered_columns:
        for (old_text, new_text) in exceptions:
            data[col] = data[col].str.replace(old_text, new_text, regex=False)

    return data


def get_data_columns():
    """

    :return:
    """

    # Reorder columns
    columns = ['kpi agency',
               'branch',
               'ceo region',
               'regional director',
               'department',
               'fte type',
               'n fte',
               'month',
               'year',
               'date',
               ]

    return columns


def transform_mapping(data, key):
    """

    :param data:
    :param key:
    :return:
    """

    if key == 'countries':

        data['agency code'] = data['agency code'].astype(int)
        data['fc code'] = data['fc code'].astype(str)

    elif key == 'departments':

        data.rename(columns={'kpi department': 'department'}, inplace=True)
        data['department fc code'] = data['department fc code'].astype(str)
        data['department'] = data['department'].astype(str)

    else:
        print(f"Issue in the mapping file, sheet {key}, while transforming data")

    data = amending_wrong_inputs(data)

    return data


def transform_data(data, country_map):
    """

    :param data:
    :param country_map:
    :return:
    """

    data.rename(columns={'kpi year month': 'date'}, inplace=True)
    data.rename(columns={'description': 'fte type'}, inplace=True)
    data.rename(columns={'current total # employees': 'n fte'}, inplace=True)

    data['n fte'] = data['n fte'].astype(float)

    # Convert 'kpi_agency' column to datetime format
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m')

    # Convert to Period with monthly frequency
    data['period'] = data['date'].dt.to_period('M')

    # Extract year and month
    data['year'] = data['period'].dt.year
    data['month'] = data['period'].dt.month

    df_merged = pd.merge(left=data, right=country_map,
                         left_on=['kpi agency', 'branch', 'ceo region', 'regional director'],
                         right_on=['kpi agency', 'branch', 'ceo region', 'regional director'], how='left')

    if len(data) == len(df_merged):
        print(f'Data were not lost in the transformation, still {len(data)} rows')
    else:
        print(f'Raw data rows: {len(data)}')
        print(f'Processed data rows: {len(df_merged)}')
        print('Data decreased after processing')
        exit()

    # Reorder columns
    data = data[get_data_columns()]

    return data


# def transform_single_data(data, country_map, department, code):
def transform_single_data(data, country_map, date):
    """

    :param data:
    :param country_map:
    :param date:
    :return:
    """

    data.rename(columns={'unnamed: 0': 'department'}, inplace=True)
    data.rename(columns={'unnamed: 1': 'fte type'}, inplace=True)

    # Convert common string NaNs to actual NaNs
    nan_values = ['nan', 'none', 'null', '', 'n/a', 'na']
    data = data.map(lambda x: np.nan if str(x).strip().lower() in nan_values else x)

    # Look for branches of the entity (agency)
    target_column = "total for all branches"
    target_index = data.columns.get_loc(target_column)
    data = data.iloc[:, :target_index]

    condition_overtime = data['fte type'] == 'hours of overtime'
    condition_nan = data['fte type'].isna()
    condition_grand_total = data['department'] == 'grand total'
    condition_fte = ~(condition_overtime | condition_nan | condition_grand_total)

    data_grand_total = data.loc[condition_grand_total, :]
    data_overtime = data.loc[condition_overtime, :]
    data_fte = data.loc[condition_fte, :]

    data_fte = data_fte.iloc[1:-1]
    data_overtime = data_overtime.iloc[:-1]

    n_translation = 2
    all_branches_data = {'fte': data_fte, 'grand total': data_grand_total, 'overtime': data_overtime}

    for key, val in all_branches_data.items():

        df_single_agency_all_branches = pd.DataFrame()
        branches = val.columns[n_translation:]
        solid_columns = val.columns[:n_translation]
        for branch in branches:
            columns = list(solid_columns) + [branch]
            df_temp = val[columns].copy()
            df_temp.rename(columns={branch: 'n fte'}, inplace=True)
            df_temp['n fte'] = df_temp['n fte'].astype(float)
            df_temp['branch'] = branch
            df_temp['kpi agency'] = country_map.loc[country_map['branch'] == branch, 'kpi agency'].iloc[0]
            df_temp = pd.merge(left=df_temp,
                               right=country_map,
                               left_on=['kpi agency', 'branch'],
                               right_on=['kpi agency', 'branch'],
                               how='left')

            df_single_agency_all_branches = df_single_agency_all_branches.reset_index(drop=True)
            df_temp = df_temp.reset_index(drop=True)
            df_single_agency_all_branches = pd.concat([df_single_agency_all_branches, df_temp])

        df_single_agency_all_branches['date'] = date['cm']['date']
        df_single_agency_all_branches['month'] = date['cm']['date'].month
        df_single_agency_all_branches['year'] = date['cm']['date'].year

        all_branches_data[key] = df_single_agency_all_branches.reset_index(drop=True)

    # Reorder columns
    data_fte = all_branches_data['fte'][get_data_columns()]
    data_grand_total = all_branches_data['grand total'][get_data_columns()]
    data_overtime = all_branches_data['overtime'][get_data_columns()]

    return data_fte, data_grand_total, data_overtime
