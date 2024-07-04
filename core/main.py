import os
import argparse
from pathlib import Path

import loading
import operations
import processing
import transforming

parser = argparse.ArgumentParser(description="Agency Grid")

parser.add_argument('--input_file', '-f',
                    type=Path,
                    dest='current_month_file',
                    help='Path to the current month data',
                    default=None, required=True)

parser.add_argument('--mapping_file', '-mf',
                    type=Path,
                    dest='mapping_file',
                    help='Path to the mapping file provided by the Finance Department',
                    default=None, required=True)

parser.add_argument('--missing_agency_files', '-maf',
                    type=str,
                    nargs='+',  # One or more values
                    dest='missing_agency_files',
                    help='A list of file paths to be processed.',
                    default=None, required=False)

parser.add_argument('--current_year', '-cy',
                    type=int,
                    dest='current_year',
                    help='Year of the current month file',
                    default=None, required=True)

parser.add_argument('--current_month', '-cm',
                    type=int,
                    dest='current_month',
                    help='Month of the current month file',
                    default=None, required=True)

parser.add_argument('--output_dir', '-o',
                    type=Path,
                    dest='output_dir',
                    help='Path to the output directory',
                    default=None, required=True)

args = parser.parse_args()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    cm_file = args.current_month_file
    mapping_file = args.mapping_file
    missing_agency_files = args.missing_agency_files
    current_year = args.current_year
    current_month = args.current_month
    output_dir = args.output_dir

    # Loading data
    df_agency = {}
    df_grid = loading.load_data(input_file=cm_file, sheet_name=None)
    df_map = {'departments': loading.load_data(input_file=mapping_file, sheet_name='Department mapping'),
              'countries': loading.load_data(input_file=mapping_file, sheet_name='Countries mapping')}
    df_date = operations.get_dates(month=current_month, year=current_year)

    # ETL
    for key, value in df_map.items():
        df_map[key] = transforming.clean_up_data(data=df_map[key])
        df_map[key] = transforming.amending_wrong_inputs(data=df_map[key])
        df_map[key] = transforming.transform_mapping(data=df_map[key], key=key)

    df_grid = transforming.clean_up_data(data=df_grid)
    df_grid = transforming.amending_wrong_inputs(data=df_grid)
    df_grid = transforming.transform_data(data=df_grid, country_map=df_map['countries'])

    # If missing agencies
    if missing_agency_files is not None:
        for file in missing_agency_files:
            agency, data_missing = loading.load_single_agency_data(input_file=file)
            print(f'{agency.capitalize()}: Data loaded')
            data_missing = transforming.clean_up_data(data=data_missing)
            print(f'{agency.capitalize()}: Data cleaned-up')
            data_missing = transforming.amending_wrong_inputs(data=data_missing)
            df_agency[agency] = {'raw': data_missing}
            # transform mapping data
            fte, total, overtime = transforming.transform_single_data(data=df_agency[agency]['raw'],
                                                                      country_map=df_map['countries'],
                                                                      date=df_date)
            df_agency[agency]['fte'] = fte
            df_agency[agency]['grand total'] = total
            df_agency[agency]['overtime'] = overtime

    missing_agencies, extra_agencies = operations.get_missing_agencies(country_map=df_map['countries'],
                                                                       current_month_data=df_grid)

    operations.print_info_about_agencies(missing_agency=missing_agencies, extra_agency=extra_agencies, date=df_date)
    operations.alerting_about_missing_agencies(single_agencies=df_agency, missing_agencies=missing_agencies)

    # Merging dataframes from single agencies to the most complete agency grid
    agency_grid = processing.merge_grid_with_single_agency(single_agencies=df_agency, agency_grid=df_grid)
    # Processing data
    agency_grid = processing.process_data(data=agency_grid,
                                          departments_map=df_map['departments'],
                                          country_map=df_map['countries'])

    agency_grid.to_excel(excel_writer=os.path.join(output_dir, f'temp_cm.xlsx'), index=False)

    consolidated_grid = processing.get_fc_file(data=agency_grid)
    consolidated_grid.to_excel(excel_writer=os.path.join(output_dir, 'fc_upload.xlsx'), index=False)
