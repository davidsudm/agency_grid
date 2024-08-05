import os
import pandas as pd


def load_data(input_file, sheet_name=None):
    """

    :param input_file:
    :param sheet_name:
    :return:
    """

    _, file_extension = os.path.splitext(input_file)

    # Check the file extension and handle accordingly
    if file_extension.lower() == '.csv':
        print("Data file loading - CSV file")
        data = pd.read_csv(filepath_or_buffer=input_file,
                           dtype={'FTEs': float},
                           converters={col: str for col in pd.read_csv(input_file, nrows=1).columns if col != 'FTEs'})
    elif file_extension.lower() in ['.xlsx', '.xls']:
        print("Mapping file loading - Excel file.")
        data = pd.read_excel(io=input_file, sheet_name=sheet_name)
    else:
        print("Unknown file extension.")
        exit()

    return data


def load_single_agency_data(input_file):
    """

    :param input_file:
    :return:
    """

    kpi_agency = pd.read_excel(io=input_file, header=None).iloc[0, 0].lower()
    data = pd.read_excel(io=input_file, skiprows=1, nrows=111)
    data = data.iloc[1:, :]

    print(f'\t - Single agency file for: {kpi_agency.title()}')
    print(f'\t - Excel file location:')
    print(f'\t - {input_file}')

    return kpi_agency, data
