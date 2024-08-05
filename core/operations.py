from datetime import datetime
from dateutil.relativedelta import relativedelta


def insert_divider_line(message, end=False):
    """

    :param message:
    :param end:
    :return:
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

    :param month:
    :param year:
    :return:
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

    :param country_map:
    :param current_month_data:
    :return:
    """

    agencies_in_map = set(sorted(country_map['kpi agency']))
    agencies_in_file = set(sorted(current_month_data['kpi agency']))

    missing_agencies = list(agencies_in_map - agencies_in_file)
    extra_agencies = list(agencies_in_file - agencies_in_map)

    return missing_agencies, extra_agencies


def print_info_about_agencies(missing_agency, extra_agency, date):
    """

    :param missing_agency:
    :param extra_agency:
    :param date:
    :return:
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

    :param single_agencies:
    :param missing_agencies:
    :return:
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

    :param country_map:
    :param agency:
    :return:
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
