import json
from google.cloud import bigquery
import pandas as pd
import yaml
from datetime import date, datetime
from pytz import timezone
import re
import numpy as np
import config as cfg
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import common_functions as util


def generate_metadata_sql(GCP_PROJECT_ID, DATASET_ID):
    '''
    Purpose: generate sql statement to pass through python request to query BQ DLP tables
    Inputs:
        (1) project ID
        (2) dataset ID
    Outputs:
        (1) SQL statement as string
    '''

    sql = (cfg.metadata_sql).format(
        GCP_PROJECT_ID,
        DATASET_ID,
        GCP_PROJECT_ID,
        DATASET_ID,
        GCP_PROJECT_ID,
        DATASET_ID,
        GCP_PROJECT_ID,
        DATASET_ID,
        GCP_PROJECT_ID,
        DATASET_ID
    )

    return sql


def column_level_sql(full_column_name):
    '''
    Purpose: generate simply top 100 non null query for column we think might have PHI
    Inputs:
        (1) full column name(project.dataset.table.field_path)
    Outputs:
        (1) sql string
    '''

    x = full_column_name.split('.')[0] + '.' + full_column_name.split('.')[1] + '.' + full_column_name.split('.')[2] + '.'

    sql = '''
    select {} as top_100
    from {}.{}.{}
    where {} is not null
    limit 100;
    '''.format(
        full_column_name.split(x)[1],
        full_column_name.split('.')[0],
        full_column_name.split('.')[1],
        full_column_name.split('.')[2],
        full_column_name.split(x)[1]
    )
    return sql


def execute_column_query(sql, file_name):
    '''
    Purpose: execute sql query for single column and return results as list
    Inputs:
        (1) sql string (column_level_sql)
    Outputs:
        (1) list of values returned from executing sql query
    '''

    df = util.read_from_bigquery(sql, file_name)

    sample_values = df['top_100'].to_list()

    return sample_values


def parse_sample_values(patterns, sample_values):
    '''
    Purpose: check list of values against list of patterns to see if patterns match
    Inputs:
        (1) list of patterns to check against
        (2) list of values to check
    Outputs:
        (1) boolean match (false = 0 matches, true = match > 0)
    '''

    for value in sample_values:
        for pattern in patterns:
            if bool(re.match(pattern, str(value))) is True:
                return True

    return False


def substring_matching(projects, datasets, tables, columns, base_dict):
    '''
    Purpose: substring match the column names for those likely to have phi
    Inputs:
        (1) list of projects from generate_metadata_sql
        (2) list of datasets from generate_metadata_sql
        (3) list of tables from generate_metadata_sql
        (4) list of field_paths from generate_metadata_sql
        (5) configuration dict
    Outputs:
        (1) updated config dict with column names attached to phi attributes
    '''

    for i in range(0, len(columns)):
        for y in columns[i]:
            for key in base_dict.keys():
                x = 0
                if len(base_dict[key]['keywords']) > 0:
                    for keyword in base_dict[key]['keywords']:
                        if x < 1:
                            if keyword in y['field_path'].lower():
                                x += 1
                                base_dict[key]['columns'].append(projects[i] + '.' + datasets[i] + '.' + tables[i] + '.' + y['field_path'])
                    if x < 1:
                        if len(base_dict[key]['combinations']) > 0:
                            xx = 0
                            for combination in base_dict[key]['combinations']:
                                for word in combination:
                                    if word in y['field_path'].lower():
                                        xx += 1
                                if xx == len(combination):
                                    base_dict[key]['columns'].append(projects[i] + '.' + datasets[i] + '.' + tables[i] + '.' + y['field_path'])
                                else:
                                    xx = 0

    return base_dict


def filter_ingored_values(base_dict):
    '''
    Purpose: columns are flagged for substring matching--this function removes columns
        from query list based on exclusionary substring matching
    Inputs:
        (1) base_dict obj with columns appended
    Outputs:
        (1) base_dict obj with columns appended filtered for exclusions 
    '''

    for key in base_dict.keys():

        kill_list = []

        for column in base_dict[key]['columns']:
            x = 0
            for ignore in base_dict[key]['ignores']:
                if x < 1:
                    if ignore in column.lower():
                        x += 1
                        kill_list.append(column)
            for table_exclude in base_dict[key]['table_excludes']:
                if x < 1:
                    if table_exclude in column.lower():
                        x += 1
                        kill_list.append(column)

        for kill in kill_list:
            if kill in base_dict[key]['columns']:
                base_dict[key]['columns'].remove(kill)

    return base_dict



def get_all_datasets_in_project(file_name, project_id):
    '''
    Purpose: collect all dataset objects within a GCP project
    Inputs:
        (1) file path of bq api token
        (2) project ID to check
    Outputs:
        (1) list containing dataset names
    '''

    client = bigquery.Client.from_service_account_json(file_name)

    datasets = list(client.list_datasets(project_id))  # Make an API request.

    output = []

    for dataset in datasets:
        output.append(dataset.dataset_id)

    return output



def main(GCP_PROJECT_ID, file_name, base_dict, dataset_list = []):
    '''
    Purpose: main function to execute PHI detection from project.dataset metadata
    Inputs:
        (1) project ID to check
        (2) dataset ID to check
        (3) file path of bq api token
        (4) base_dict = config dict storing attributes to check & common patterns
    Outputs:
        (1) df containing: date, location, phi type, boolean pattern match
    '''

    hist_list = check_historic_results(file_name, GCP_PROJECT_ID)

    if len(dataset_list) < 1:

        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': dataset collection started for ' + GCP_PROJECT_ID)
        datasets = get_all_datasets_in_project(file_name, GCP_PROJECT_ID)
        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': dataset collection ended for ' + GCP_PROJECT_ID)

    else:
        datasets = dataset_list

    error_list = []
    dataset_num = 0

    for dataset in datasets:

        # collect metadata for project.dataset
        dataset_num += 1

        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': metadata collection started for ' + str(dataset)+ ' (' + str(dataset_num) + '/' + str(len(datasets)) + ')')
        try:
            client = bigquery.Client.from_service_account_json(file_name)
            df = client.query(generate_metadata_sql(GCP_PROJECT_ID, dataset)).to_dataframe()
            print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': metadata collection ended for ' + str(dataset))
        except:
            print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': error collecting metadata for ' + str(dataset))

        # check column names collected from metadata against phi attributes searching for

        base_dict = substring_matching(
            df['table_catalog'].to_list(),
            df['table_schema'].to_list(),
            df['table_name'].to_list(),
            df['column'].to_list(),
            base_dict
        )

        updated_dict = filter_ingored_values(base_dict)

    output = []

    for key in updated_dict.keys():
        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': pattern matching started for: ' + key)
        x = 0
        for column in updated_dict[key]['columns']:
            x += 1
            print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ' ' + key + ': ' + str(column) + ' (' + str(x) + '/' + str(len(updated_dict[key]['columns'])) + ')')
            if len(hist_list) != 0 and column in hist_list.keys():
                output.append(
                    {
                        'date': date.today(),
                        'project': column.split('.')[0],
                        'dataset': column.split('.')[1],
                        'table': column.split('.')[2],
                        'field_path': column.split(column.split('.')[0] + '.' + column.split('.')[1] + '.' + column.split('.')[2] + '.')[1],
                        'phi_type': hist_list[column][0],
                        'pattern_match': bool(hist_list[column][1])
                    }
                )
            else:
            # for each column, collect first 100 non null values and check values against common patterns

                try:

                    sample_values = execute_column_query(column_level_sql(column), file_name)
                    output.append(
                        {
                            'date': date.today(),
                            'project': column.split('.')[0],
                            'dataset': column.split('.')[1],
                            'table': column.split('.')[2],
                            'field_path': column.split(column.split('.')[0] + '.' + column.split('.')[1] + '.' + column.split('.')[2] + '.')[1],
                            'phi_type': key,
                            'pattern_match': parse_sample_values(updated_dict[key]['patterns'], sample_values)
                        }
                    )
                except:
                    error_list.append(column)
        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': pattern matching ended for ' + key)

    print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ': pattern matching ended')

    df = pd.DataFrame(output)

    return df, error_list


def check_historic_results(file_name, project_id):
    '''
    Purpose: check to see if script has been run on this project in past. if yes, inherit results
    Inputs:
        (1) file path of bq api token
        (2) project ID to check
    Outputs:
        (1) df containing most recent run results
    '''
    output = {}
    print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ' collecting historical data')

    client = bigquery.Client.from_service_account_json(file_name)

    try:
        df = client.query((cfg.hist_sql).format('"' + project_id + '"', '"' + project_id + '"')).to_dataframe()
        hist_list = df['column_name'].to_list()
        if len(hist_list) > 0:
            for item in hist_list:
                output[item.split('?_?')[0]] = [item.split('?_?')[1], item.split('?_?')[2]]
        return output

    except:
        print(str(datetime.now(timezone('EST')).strftime('%Y-%m-%d %H:%M:%S')) + ' error collecting historical data')
        return output
