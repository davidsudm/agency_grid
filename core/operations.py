from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_dates(month, year):
    """

    :param month:
    :param year:
    :return:
    """

    date_dict = {}

    current_date = datetime(year=year, month=month, day=1)
    previous_date = current_date - relativedelta(months=1)

    for (tag, date) in [('pm', previous_date), ('cm', current_date)]:
        date_dict[tag] = {}
        date_dict[tag]['date'] = date
        date_dict[tag]['long'] = date.strftime("%B %Y")
        date_dict[tag]['short'] = date.strftime("%b %Y")
        date_dict[tag]['number'] = date.strftime("%Y %m")

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

    for (text, agency_type) in [('missing', missing_agency),
                                ('additional', extra_agency)]:
        if len(agency_type) > 0:
            agencies = [item.capitalize() for item in agency_type]
            print(f'{text} agencies in current month ({date['cm']['long']}):')
            print(', '.join(agencies))
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

    if len(missing_files) > 0:
        print('Among the missing agencies, the following ones were not loaded via a single file')
        agencies = [item.capitalize() for item in missing_files]
        print('>>>>   ' + ', '.join(agencies) + '   <<<<')
    else:
        print('Single files matched the agency files')

    if len(unneeded_files) > 0:
        print('More agency single files than needed were input. REMOVE THEM!!')
        agencies = [item.capitalize() for item in unneeded_files]
        print('>>>>   ' + ', '.join(agencies) + '   <<<<')
        # exit()
    else:
        print('Single files matched the agency files')

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
