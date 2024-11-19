from datetime import datetime
from dateutil.relativedelta import relativedelta


def insert_divider_line(message, end=False):
    """
    Prints a divider line with a message for better console output readability.

    :param message: The message to print.
    :param end:     Whether this is the ending divider.
    :return: None (prints to console).
    """

    line = "*************************************************"
    if not end:
        print(f"{line} -- {message} -- {line}")
    else:
        print(f"{line} -- {message} -- {line}")
        print("\n")

    return None


def get_dates(month, year):
    """
    Generates a dictionary with various formatted date strings for the given month and year.

    :param month:   The month number.
    :param year:    The year number.
    :return: A dictionary with formatted date strings.
    """

    date_dict = {}
    current_date = datetime(year=year, month=month, day=1)
    date_dict['date'] = current_date
    date_dict['long'] = current_date.strftime("%B %Y")
    date_dict['short'] = current_date.strftime("%b %Y")
    date_dict['number'] = current_date.strftime("%Y %m")

    return date_dict


def get_missing_agencies(country_map, current_month_data):
    """
    Identifies missing and extra agencies by comparing the country map and current month data.

    :param country_map:         A DataFrame with agency data from the mapping.
    :param current_month_data:  A DataFrame with agency data from the current month's data.
    :return: Two lists containing missing and extra agencies.
    """

    agencies_in_map = set(sorted(country_map['kpi agency']))
    agencies_in_file = set(sorted(current_month_data['kpi agency']))

    missing_agencies = list(agencies_in_map - agencies_in_file)
    extra_agencies = list(agencies_in_file - agencies_in_map)

    return missing_agencies, extra_agencies


def print_info_about_agencies(missing_agency, extra_agency, date):
    """
    Prints information about missing and extra agencies based on the comparison of the mapping file and current month
    data.

    :param missing_agency:  A list of agencies that are in the mapping file but missing in the current month data.
    :param extra_agency:    A list of agencies that are in the current month data but not in the mapping file.
    :param date:            A dictionary containing the formatted date information (long, short, etc.).
    :return: None (prints information to the console).
    """

    for (text, agency_type) in [('MISSING', missing_agency),
                                ('ADDITIONAL', extra_agency)]:
        if len(agency_type) > 0:
            agencies = [item.capitalize() for item in agency_type]
            print(f'{text} agencies in current month ({date['long']}):')
            for agent in agencies:
                print('\t', '-', agent)
        elif len(agency_type) == 0:
            print(f'No {text} agencies using the current mapping as our benchmark')
        else:
            print(f'Number of {text} agencies MUST BE a whole number equal or greater than zero')
            exit()

    return None


def alerting_about_missing_agencies(single_agencies, missing_agencies):
    """
    Alerts about missing or unneeded single agency files by comparing the provided single agency data with the list of
    missing agencies.

    :param single_agencies:     A dictionary of single agency data loaded from files.
    :param missing_agencies:    A list of agencies missing from the main dataset.
    :return: None (prints alerts to the console).
    """

    agencies_in_files = set(single_agencies.keys())
    missing_agencies = set(missing_agencies)

    missing_files = missing_agencies - agencies_in_files
    unneeded_files = agencies_in_files - missing_agencies

    print('AFTER single files integration:')
    if len(missing_files) > 0:
        agencies = [item.capitalize() for item in missing_files]
        print('\t', 'Among the missing agencies, the following ones were not loaded via a single file.')
        for agent in agencies:
            print('\t', f'>>>>   {agent}   <<<<')
    else:
        print('NO MISSING agencies were found.')
        print('The agencies in the mapping file match the agencies in Grid and Single files.')

    if len(unneeded_files) > 0:
        print('AFTER SINGLE AGENCIES integration, MORE AGENCIES single files than needed were input!')
        print('Solution: REMOVE THEM or UPDATE THE MAPPING!')
        agencies = [item.capitalize() for item in unneeded_files]
        for agent in agencies:
            print('\t', f'>>>>   {agent}   <<<<')
        # exit()

    else:
        print('AFTER SINGLE AGENCIES integration, MORE AGENCIES single files than needed were input!')
        print('NO ADDITIONAL agencies were found.')
        print('The agencies in the mapping file match the agencies in Grid and Single files.')

    return None


def get_agency_code(country_map, agency):
    """
    Retrieves the FC code corresponding to a specific agency from the country map. If multiple or no FC codes are found,
    it raises an error.

    :param country_map:     A DataFrame containing the mapping of agencies to FC codes.
    :param agency:          The name of the agency for which to find the FC code.
    :return: The FC code associated with the agency. If no or multiple codes are found, an error is printed and the
    program exits.
    """

    condition = country_map['kpi_agency'] == agency
    fc_codes = set(country_map.loc[condition, 'fc_code'].tolist())

    if len(fc_codes) == 0:
        print(f'Impossible to find a FC CODE corresponding to {agency}.')
        exit()
    elif len(fc_codes) > 1:
        print('More than one FC CODE found:')
        print(f"{', '.join(fc_codes)}")
        print('Impossible to decide.')
        exit()
    else:
        entity_fc_code = fc_codes.pop()

    return entity_fc_code
