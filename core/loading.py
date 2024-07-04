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
        data = pd.read_csv(filepath_or_buffer=input_file)
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
    data = pd.read_excel(io=input_file, skiprows=1)

    print(f'Single agency file for: {kpi_agency}')
    print(f'Excel file location:')
    print(input_file)

    return kpi_agency, data
