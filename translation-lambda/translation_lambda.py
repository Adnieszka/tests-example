import json
import os
import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime


import logging

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_LEVEL', logging.INFO))

DESTINATION_BUCKET_NAME = os.environ.get(
    "DESTINATION_BUCKET_NAME")

DESTINATION_LOCATION_PREFIX = os.environ.get(
    "DESTINATION_LOCATION_PREFIX")

ERROR_BUCKET_NAME = os.environ.get("ERROR_BUCKET_NAME")

ERROR_LOCATION_PREFIX = os.environ.get(
    "ERROR_LOCATION_PREFIX")

REGION = os.environ.get("REGION")

SOURCE_LANGUAGE_CODE = os.environ.get("SOURCE_LANGUAGE_CODE")

TARGET_LANGUAGE_CODE = os.environ.get("TARGET_LANGUAGE_CODE")

TRANSLATION_BOTO_CLIENT_MAX_ATTEMPTS = int(
    os.environ.get("TRANSLATION_BOTO_CLIENT_MAX_ATTEMPTS") or '10')

TRANSLATION_BOTO_CLIENT_READ_TIMEOUT = int(
    os.environ.get("TRANSLATION_BOTO_CLIENT_READ_TIMEOUT") or '3')

TRANSLATION_BOTO_CLIENT_CONNECT_TIMEOUT = int(
    os.environ.get("TRANSLATION_BOTO_CLIENT_CONNECT_TIMEOUT") or '3')


translation_boto_Client_config = Config(retries={
                                        'max_attempts': TRANSLATION_BOTO_CLIENT_MAX_ATTEMPTS,
                                        'mode': 'standard'
                                        },
                                        region_name=REGION,
                                        read_timeout=TRANSLATION_BOTO_CLIENT_READ_TIMEOUT,
                                        connect_timeout=TRANSLATION_BOTO_CLIENT_CONNECT_TIMEOUT
                                        )


boto_translation_client = boto3.client(
    'translate', config=translation_boto_Client_config)


def extract_path(record):
    bucket_name = record['bucket']
    key_name = record['key']
    file_name = (key_name.split("/")[-1]).split('.')[0]

    return bucket_name, key_name, file_name


def translate(record):

    logger.info('Extracting file path...')
    try:
        source_bucket, source_key, file_name = extract_path(record=record)
    except Exception as exception:
        return {'path_extraction_error': str(exception), 'record': json.dumps(record, indent=4)}
    logger.info(
        f'Working with file {file_name} at path: s3://{source_bucket}/{source_key}')

    logger.info(f'Populating dateframe with records...')
    try:
        df = pd.read_parquet(f's3://{source_bucket}/{source_key}')
    except Exception as exception:
        return {'data_reading_error': str(exception), 'bucket': source_bucket, 'key': source_key}
    logger.info(f'Dataframe populated: \n {df[:5]}')

    logger.info('Translating dataframe...')
    translated, errors = translate_dataframe(df)

    destination_string = f's3://{DESTINATION_BUCKET_NAME}/{DESTINATION_LOCATION_PREFIX}/{file_name}.parquet.gzip'
    result = {'bucket': DESTINATION_BUCKET_NAME,
              'key': f'{DESTINATION_LOCATION_PREFIX}/{file_name}.parquet.gzip'}

    try:
        pd.DataFrame(translated).to_parquet(
            path=destination_string, compression='gzip')
        result['translation_status'] = 'OK'
    except Exception as exception:
        result['writing_status'] = 'Errors occured'
        result['writing_error_messages'] = str(exception)

        return result

    try:
        if len(errors):
            error_destination_string = f's3://{ERROR_BUCKET_NAME}/{ERROR_LOCATION_PREFIX}/{file_name}.parquet.gzip'
            pd.DataFrame(errors).to_parquet(
                path=error_destination_string, compression='gzip')
            result['translation_status'] = 'Errors occured'
            result['translation_error_messages'] = {
                'error_file': error_destination_string, 'error_messages': errors}
        result['writing_status'] = 'OK'
    except Exception as exception:
        result['writing_status'] = 'Errors occured'
        result['writing_error_messages'] = str(exception)

        return result

    return result


def translate_dataframe(df):

    translated = []
    translation_errors = []

    logger.info('Translating separate rows...')
    for idx, row in df.iterrows():
        try:
            translated.append(translate_row(row))

        except Exception as translate_exception:

            translation_errors.append(
                {'ID': row['ID'], 'original_text': row['review'], 'error_message': str(translate_exception)})

    return translated, translation_errors


def translate_row(row):

    try:
        response = boto_translation_client.translate_text(
            Text=row['review'], SourceLanguageCode=SOURCE_LANGUAGE_CODE, TargetLanguageCode=TARGET_LANGUAGE_CODE)

        return {'ID': row['ID'], 'original_review_language': response.get('SourceLanguageCode'), 'review_translation': response.get('TranslatedText')}

    except Exception as ex:
        raise Exception(f'{ex}')


def lambda_handler(event, context):

    translation_output = [translate(record=record)
                          for record in event]
    logger.info(translation_output)
    return translation_output
