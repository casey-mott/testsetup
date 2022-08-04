import json
from google.cloud import bigquery
import pandas as pd
import yaml
from datetime import date, datetime
from pytz import timezone
import re
import numpy as np
import config as cfg
import functions as f
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import common_functions as util

### waiting on https://jira.hioscar.com/browse/ART-33910 to query all projects
### fixed list for now based on svc acct access

projects = [
    'security-263001',
    'oscarproductanalytics'
]

base_df, base_error_list = f.main(
    projects[0],
    cfg.file_name,
    cfg.base_dict,
    #dataset_list = ['periscope']
)

for project in projects[1:]:
    df, error_list = f.main(
        'security-263001',
        cfg.file_name,
        cfg.base_dict,
        #dataset_list = ['periscope']
    )

    for item in error_list:
        base_error_list.append(item)

    base_df = pd.concat([base_df, df])

util.write_to_bigquery(
    'security-263001.dlp_test.dlp_job_results',
    base_df,
    cfg.file_name
)
