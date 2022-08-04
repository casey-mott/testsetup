import json
from google.cloud import bigquery
import pandas as pd
import yaml
from datetime import date
import os


def get_roles_from_iam_policy(project):
    '''
    Purpose: execute terminal cmd to collect get-iam-policy for project and save as yaml
    Input: (1) project ID
    Output: (1) yaml file, (2) df of parsed yaml
    '''
    var=project
    #!gcloud projects get-iam-policy $var > output.yaml
    cmd = "gcloud projects get-iam-policy {} > output.yaml".format(var)
    os.system(cmd)

    member_list = []

    with open(r'output.yaml') as file:
        documents = yaml.full_load(file)

        for item in documents['bindings']:
            for member in item['members']:
                member_list.append(
                    {
                        'project': project,
                        'role': item['role'],
                        'member': member
                    }
                )


        member_df = pd.DataFrame(member_list)

    return member_df


def get_iam_policies_for_all_projects(projects):
    '''
    Purpose: collect all iam policies and parse for members for BQ projects
    Inputs: (1) list of projects
    Outputs: (1) df: projects | members for all proj
    '''
    try:
        base_df = get_roles_from_iam_policy(projects[0])
        print('Success: ' + projects[0])
    except:
        print('Error: ' + projects[0])

    if len(projects) > 1:

        for project in projects[1:]:
            try:
                df = get_roles_from_iam_policy(project)
                base_df = pd.concat([base_df, df])
                print('Success: ' + project)
            except:
                print('Error: ' + project)

    return base_df
