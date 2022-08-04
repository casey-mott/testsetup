from google.cloud import bigquery


def write_to_bigquery(table_name, df, file_name):
    '''
    Purpose: write dataframe to BigQuery table
    Inputs:
        (1) table name to write to,
        (2) dataframe to write,
        (3) file location for authentication
    Outputs: none
    '''

    client = bigquery.Client.from_service_account_json(file_name)

    try:

        job = client.load_table_from_dataframe(df, table_name)

        print('Success')

    except:

        print('Error writing to BigQuery')
    return True


def read_from_bigquery(sql_string, file_name):
    '''
    Purpose: execute SQL query to collect data from BigQuery
    Inputs:
        (1) SQL query as string (i.e. 'select * from security-263001.dlp_test.table_name')
        (2) file location for authentication
    Outputs:
        (1) df result from SQL query execution
    '''

    client = bigquery.Client.from_service_account_json(file_name)

    df = client.query(sql_string).to_dataframe()

    return df
