import json
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import yaml
from datetime import date
import config as cfg
import functions as f
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import common_functions as util


df = f.get_iam_policies_for_all_projects(cfg.projects)

util.write_to_bigquery(
    cfg.output_table,
    df,
    cfg.file_name
)
