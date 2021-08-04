import os
import json
import pandas as pd


def lambda_handler(events, context):
    bucket_out_name = os.environ["DATA_LAKE_NAME"]
    key_out_prefix_name = os.environ.get("KEY_OUT_PREFIX")
    result = []
    print(events)
    for record in events['Input']['Records']:
        bucket_in_name, key_in_name = read_variables(record)

        file_out_name = create_file_name(key_in_name)
        input_data_df = pd.read_csv(f's3://{bucket_in_name}/{key_in_name}')
        key_out_name = f'{key_out_prefix_name}/{file_out_name}.parquet.gzip'
        input_data_df.to_parquet(f's3://{bucket_out_name}/{key_out_name}', compression='gzip')
        outputs = {'bucket': bucket_out_name, 'key': key_out_name}
        result.append(outputs)

    return result


def read_variables(record):
    try:
        bucket_in_name = record['s3']['bucket']['name']
        key_in_name = record['s3']['object']['key']

        return bucket_in_name, key_in_name
    except Exception as e:
        print('Missing s3 bucket name or object key')

        raise e


def create_file_name(file_in_name):
    file_split_name = file_in_name.split("/")[-1]
    file_out_name = file_split_name.split('.')[0]

    return file_out_name
