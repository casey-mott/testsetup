file_name = ''

metadata_sql = '''
WITH
  -- Parse out and clean up the option_value column from TABLE_OPTIONS
  cleaned_table_options AS (
    SELECT
      t.table_catalog,
      t.table_schema,
      t.table_name,
      t.table_type,
      t_o.option_name,
      IF
        (t_o.option_name = "labels",
          SPLIT( REPLACE( REPLACE( REPLACE(
            SUBSTR(t_o.option_value, 2, LENGTH(option_value)-4),
            "STRUCT(\\"", '' ),
            "\\", \\"", ":" ),
            "\\"), ", "," )
          ),
        NULL) AS new_option_value,
      t_o.option_value AS original_option_value
    FROM
      {}.{}.INFORMATION_SCHEMA.TABLES t -- TODO: PARAMETERIZE: Set to your dataset for TABLE_OPTIONS
    LEFT JOIN
      {}.{}.INFORMATION_SCHEMA.TABLE_OPTIONS t_o -- TODO: PARAMETERIZE: Set to your dataset for TABLE_OPTIONS
    ON
      t.table_catalog = t_o.table_catalog
      AND t.table_schema = t_o.table_schema
      AND t.table_name = t_o.table_name ),

  -- Create arrays and structs for the options and values.
  table_options_struct AS (
    SELECT
      cleaned_table_options.table_catalog,
      cleaned_table_options.table_schema,
      cleaned_table_options.table_name,
      cleaned_table_options.table_type,
    IF
      (option_name = "labels",
        STRUCT(ARRAY_AGG(STRUCT(SPLIT(unnested_option_value, ":")[OFFSET(0)] AS name,
          SPLIT(unnested_option_value, ":")[OFFSET(1)] AS value) RESPECT NULLS) AS label_object),
        NULL) AS label,
    IF
      (option_name != "labels",
        ANY_VALUE(STRUCT(option_name AS name,
            original_option_value AS value)),
        NULL) AS option,
    FROM
      cleaned_table_options
    LEFT JOIN
      UNNEST(new_option_value) AS unnested_option_value
    GROUP BY
      table_catalog,
      table_schema,
      table_name,
      table_type,
      option_name ),

  -- Aggregate column information.
  COLUMNS AS (
  SELECT
    table_name,
    ARRAY_AGG(STRUCT(
        column_name AS name,
        field_path AS field_path,
        data_type AS type,
        description)) column
  FROM
    {}.{}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS -- TODO: PARAMETERIZE: Set to your dataset for COLUMN_FIELD_PATHS
  GROUP BY
    table_name ),
  table_metadata AS (
    SELECT
      tos.table_catalog,
      tos.table_schema,
      tos.table_name,
      tos.table_type,
      STRUCT(
        ARRAY_AGG(tos.label IGNORE NULLS) as label,
        ARRAY_AGG(tos.option IGNORE NULLS) as option
      ) as table_options,
      ANY_VALUE(columns.column) as column
    FROM
      table_options_struct tos
    LEFT JOIN
      columns
    ON
      tos.table_name = COLUMNS.table_name
    GROUP BY
      tos.table_catalog,
      tos.table_schema,
      tos.table_name,
      tos.table_type
  ),
  log_table_details AS (
    SELECT DISTINCT
      protopayload_auditlog.authenticationInfo.principalEmail AS UserId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
      ReferencedTables.projectId AS ref_table_project_id,
      ReferencedTables.datasetId AS ref_table_dataset_id,
      ReferencedTables.tableId AS ref_table_table_id,
      ReferencedViews.projectId AS ref_view_project_id,
      ReferencedViews.datasetId AS ref_view_dataset_id,
      ReferencedViews.tableId AS ref_view_table_id,
      insertId,
    FROM
      `oscarlogarchive.gcp_logs.cloudaudit_googleapis_com_data_access`
    LEFT JOIN
      UNNEST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables) AS ReferencedTables
    LEFT JOIN
      UNNEST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedViews) AS ReferencedViews
    WHERE true
      AND DATE(timestamp) BETWEEN "2022-04-28" AND "2022-05-03"
      AND (
        (ReferencedTables.projectId = '{}' AND ReferencedTables.datasetId = '{}')
        OR
        (ReferencedViews.projectId = '{}' AND ReferencedViews.datasetId = '{}')
      )
  ),
  results_ungrouped AS (
    SELECT
      tm.table_catalog,
      tm.table_schema,
      tm.table_name,
      tm.table_type,
      tm.table_options,
      tm.column,
      COALESCE(ltt.UserId,ltv.UserId) AS ConsumerId,
      COALESCE(ltt.insertId, ltv.insertId) AS InsertId,
    FROM
      table_metadata AS tm
    LEFT JOIN
      log_table_details as ltt
      ON tm.table_catalog = ltt.ref_table_project_id
      AND tm.table_schema = ltt.ref_table_dataset_id
      AND tm.table_name = ltt.ref_table_table_id
    LEFT JOIN
      log_table_details as ltv
      ON tm.table_catalog = ltv.ref_view_project_id
      AND tm.table_schema = ltv.ref_view_dataset_id
      AND tm.table_name = ltv.ref_view_table_id
  ),
  results_grouped AS (
    SELECT
      table_catalog,
      table_schema,
      table_name,
      table_type,

      ARRAY_AGG(DISTINCT results_ungrouped.ConsumerId IGNORE NULLS ORDER BY results_ungrouped.ConsumerId) AS unique_consumers,
      -- STRING_AGG(DISTINCT results_ungrouped.ConsumerId ORDER BY results_ungrouped.ConsumerId) AS unique_consumers,
      ANY_VALUE(results_ungrouped.InsertId) AS InsertId
    FROM
      results_ungrouped
    GROUP BY
      table_catalog,
      table_schema,
      table_name,
      table_type
  )
SELECT
  results_grouped.*,
  table_metadata.table_options,
  table_metadata.column
FROM
  results_grouped
LEFT JOIN
  table_metadata
  USING(table_catalog, table_schema, table_name)
  ORDER BY ARRAY_LENGTH(unique_consumers) DESC;
'''

base_dict = {
    'email': {
        'keywords': [
            'email'
        ],
        'combinations': [],
        'columns': [],
        'patterns': [
            '^([a-zA-Z0-9_\-]+\.)*[a-zA-Z0-9_\-]+@([a-zA-Z0-9_\-]+\.)+(com|org|edu|net|ca|au|coop|de|ee|es|fm|fr|gr|ie|in|it|jp|me|nl|nu|ru|uk|us|za)$'
        ],
        'ignores': [
            'broker',
            'campaign',
            'click',
            'comms',
            'consent',
            'count',
            'deliver',
            'dt',
            'has',
            'month',
            'notification',
            'opened',
            'provider',
            'status',
            'subject',
            'type',
            'ts',
            'verif',
            'voice'

        ],
        'table_excludes': []
    },
    'first_name': {
        'keywords': [
            'first_name'
        ],
        'combinations': [
            [
                'first',
                'name'
            ]
        ],
        'columns': [],
        'patterns': [
            '([A-Z][a-zA-Z]*)',
            '([a-zA-Z]*)'
        ],
        'ignores': [
            'group',
            'plan',
            'provider'
        ],
        'table_excludes': []
    },
    'last_name': {
        'keywords': [
            'last_name'
        ],
        'combinations': [
            [
                'last',
                'name'
            ]
        ],
        'columns': [],
        'patterns': [
            '([A-Z][a-zA-Z]*)',
            '([a-zA-Z]*)'
        ],
        'ignores': [
            'group',
            'plan',
            'provider'
        ],
        'table_excludes': []
    },
    'full_name': {
        'keywords': [
            'full_name',
            'member_name',
            'person_name'
        ],
        'combinations': [
            [
                'full',
                'name'
            ],
            [
                'person',
                'name'
            ],
            [
                'member',
                'name'
            ]
        ],
        'columns': [],
        'patterns': [
            "/^[a-z ,.'-]+$/i"
        ],
        'ignores': [
            'group',
            'plan',
            'provider'
        ],
        'table_excludes': []
    },
    'age': {
        'keywords': [
            'age'
        ],
        'combinations': [],
        'columns': [],
        'patterns': [
            '^[1-9][0-9]$',
            '^[0-9]$'
        ],
        'ignores': [
            'agency',
            'agent',
            'average',
            'coverage',
            'engage',
            'image',
            'language',
            'manage',
            'messag',
            'package',
            'page',
            'percentage',
            'stage',
            'usage'
        ],
        'table_excludes': [
            'oscarproductanalytics.playground_db.visits_'
        ]
    },
    'date_of_birth': {
        'keywords': [
            'dob'
        ],
        'combinations': [
            [
                'date',
                'birth'
            ]
        ],
        'columns': [],
        'patterns': [
            '[0-9]{2}/[0-9]{2}/[0-9]{4}',
            '[0-9]{1}/[0-9]{1}/[0-9]{4}',
            '[0-9]{2}/[0-9]{2}/[0-9]{2}',
            '[0-9]{1}/[0-9]{1}/[0-9]{2}',
            '[0-9]{4}/[0-9]{2}/[0-9]{2}',
            '[0-9]{4}/[0-9]{1}/[0-9]{1}',
            '[0-9]{2}-[0-9]{2}-[0-9]{4}',
            '[0-9]{1}-[0-9]{1}-[0-9]{4}',
            '[0-9]{2}-[0-9]{2}-[0-9]{2}',
            '[0-9]{1}-[0-9]{1}-[0-9]{2}',
            '[0-9]{4}-[0-9]{2}-[0-9]{2}',
            '[0-9]{4}-[0-9]{1}-[0-9]{1}'
        ],
        'ignores': [],
        'table_excludes': []
    },
    'phone_number': {
        'keywords': [
            'phone'
        ],
        'combinations': [
            [
                'contact',
                'number'
            ],
            [
                'member',
                'number'
            ]
        ],
        'columns': [],
        'patterns': [
            '\(?\d{3}\)?-? *\d{3}-? *-?\d{4}'
        ],
        'ignores': [
            'facility',
            'provider'
        ],
        'table_excludes': []
    },
    'gender': {
        'keywords': [
            'gender',
            'sex'
        ],
        'combinations': [],
        'columns': [],
        'patterns': [
            '^(?:m|M|male|Male|MALE|f|F|female|Female|FEMALE)$'
        ],
        'ignores': [],
        'table_excludes': [
            'oscarproductanalytics.playground_db.visits_'
        ]
    },
    'social': {
        'keywords': [
            'social',
            'ssn'
        ],
        'combinations': [
            [
                'last',
                'four'
            ]
        ],
        'columns': [],
        'patterns': [
            '(?!666|000|9\\d{2})\\d{3}-(?!00)\\d{2}-(?!0{4})\\d{4}$',
            '(?!666|000|9\\d{2})\\d{3} (?!00)\\d{2} (?!0{4})\\d{4}$',
            '(?!666|000|9\\d{2})\\d{3}(?!00)\\d{2}(?!0{4})\\d{4}$'
        ],
        'ignores': [
            'search'
        ],
        'table_excludes': []
    },
    'ip_address': {
        'keywords': [
            'ip_address',
            'ipaddress'
        ],
        'combinations': [
            [
                'ip',
                'address'
            ]
        ],
        'columns': [],
        'patterns': [
            '\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}\b'
        ],
        'ignores': [
            'zip'
        ],
        'table_excludes': []
    },
    'street_address': {
        'keywords': [
            'address',
            'street'
        ],
        'combinations': [],
        'columns': [],
        'patterns': [
            '\\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\\.?',
            '\\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:avenue|lane|road|boulevard|drive|street|ave|dr|rd|blvd|ln|st)\\.?'
        ],
        'ignores': [
            'doc',
            'email',
            'facility',
            'ip',
            'provider'
        ],
        'table_excludes': []
    },
}

hist_sql = '''
with base as (
    select
      project
      , date
      , concat(project, '.', dataset, '.', table, '.', field_path, '?_?', phi_type, '?_?', pattern_match) as column_name
    from security-263001.dlp_test.dlp_job_results
    where project = {}
), date_max as (
    select
      project
      , max(date) as date_filter
    from security-263001.dlp_test.dlp_job_results
    where project = {}
    group by 1
)
select
    column_name
from base
left join date_max
    on base.project = date_max.project
where base.date = date_max.date_filter;
'''
