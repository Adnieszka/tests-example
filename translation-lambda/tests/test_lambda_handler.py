from typing import Dict
import unittest
import json
from parameterized import parameterized
from unittest import mock


import site
import sys
import logging

site.addsitedir("terraform/solution/lambda_translation/translation-lambda/src")
# Always appends to end
site.addsitedir("terraform/solution/lambda_translation/translation-lambda/tests")
print(sys.path)

import translation_lambda


class TestLambdaFunction(unittest.TestCase):
    def setUp(self):
        self._events_path = (
            "terraform/solution/lambda_translation/translation-lambda/tests/events/"
        )

    def test_extract_path_success(self):
        # GIVEN:
        record = {
            "bucket": "s3-bucket-name",
            "key": "data/review_data.parquet.gzip",
        }
        # WHEN:
        source_bucket, source_key, file_name = translation_lambda.extract_path(record)
        # THEN:
        self.assertDictEqual({"bucket": source_bucket, "key": source_key}, record)
        self.assertEqual(file_name, (record["key"].split("/")[-1]).split(".")[0])

    def test_extract_path_no_attributes(self):
        # GIVEN:
        record = {"key": "data/review_data.parquet.gzip"}
        # WHEN:
        with self.assertRaises(Exception) as exception_context_manager:
            translation_lambda.extract_path(record)
            self.assertEqual(exception_context_manager.exception, KeyError("bucket"))

    @mock.patch(
        "translation_lambda.boto_translation_client.translate_text",
        return_value={"SourceLanguageCode": "pl", "TranslatedText": "hello"},
    )
    @mock.patch("translation_lambda.SOURCE_LANGUAGE_CODE", "auto")
    @mock.patch("translation_lambda.TARGET_LANGUAGE_CODE", "en")
    def test_translate_row_return_success(self, boto_translation_client):
        # GIVEN:
        row = {"ID": 0, "review": "cześć"}
        # WHEN:
        translation = translation_lambda.translate_row(row)
        # THEN:
        translation_lambda.boto_translation_client.translate_text.assert_called_once_with(
            Text="cześć", SourceLanguageCode="auto", TargetLanguageCode="en"
        )
        self.assertDictEqual(
            translation,
            {"ID": 0, "original_review_language": "pl", "review_translation": "hello"},
        )

    @mock.patch("translation_lambda.boto_translation_client.translate_text")
    def test_translate_row_return_error(self, boto_translation_client):
        # GIVEN:
        row = {"ID": 0, "review": "cześć"}
        boto_translation_client.side_effect = mock.Mock(side_effect=Exception("Test"))
        # THEN:
        with self.assertRaises(Exception) as exception_context_manager:
            translation_lambda.translate_row(row)
            translation_lambda.boto_translation_client.translate_text.assert_called_once_with(
                Text="cześć", SourceLanguageCode="auto", TargetLanguageCode="en"
            )
            self.assertEqual(exception_context_manager.exception, Exception("Test"))

    @parameterized.expand(
        [
            [
                "all_success",
                [[0, {"ID": 0, "review": "cześć"}], [1, {"ID": 1, "review": "pa"}]],
                [
                    {
                        "ID": 0,
                        "original_review_language": "pl",
                        "review_translation": "hello",
                    },
                    {
                        "ID": 1,
                        "original_review_language": "pl",
                        "review_translation": "bye",
                    },
                ],
                [
                    {
                        "ID": 0,
                        "original_review_language": "pl",
                        "review_translation": "hello",
                    },
                    {
                        "ID": 1,
                        "original_review_language": "pl",
                        "review_translation": "bye",
                    },
                ],
                [],
            ],
            [
                "translation_error_occured",
                [[0, {"ID": 0, "review": "cześć"}], [1, {"ID": 1, "review": "pa"}]],
                [
                    Exception("Test"),
                    {
                        "ID": 1,
                        "original_review_language": "pl",
                        "review_translation": "bye",
                    },
                ],
                [
                    {
                        "ID": 1,
                        "original_review_language": "pl",
                        "review_translation": "bye",
                    }
                ],
                [{"ID": 0, "original_text": "cześć", "error_message": "Test"}],
            ],
        ]
    )
    @mock.patch("translation_lambda.translate_row", autospec=True)
    @mock.patch("translation_lambda.pd.DataFrame.iterrows")
    def test_translate_dataframe(
        self,
        name,
        iterrows_outputs,
        translate_row_outputs,
        translated_list_result,
        translation_error_list_result,
        translate_row,
        iterrows,
    ):
        # GIVEN:
        df = mock.MagicMock()
        iterrows = mock.Mock(return_value=iterrows_outputs)
        translation_lambda.translate_row = mock.Mock(side_effect=translate_row_outputs)
        df.iterrows = iterrows
        # WHEN:
        translated, translation_errors = translation_lambda.translate_dataframe(df)
        self.assertListEqual(translated, translated_list_result)
        self.assertListEqual(translation_errors, translation_error_list_result)

    @parameterized.expand(
        [
            [
                "write_with_success",
                [None, None],
                [
                    mock.call(
                        path="s3://destination_bucket/destination_key/file_name.parquet.gzip",
                        compression="gzip",
                    ),
                    mock.call(
                        path="s3://error_bucket/error_key/file_name.parquet.gzip",
                        compression="gzip",
                    ),
                ],
                {
                    "bucket": "destination_bucket",
                    "key": "destination_key",
                    "translation_status": "Errors occured",
                    "translation_error_messages": {
                        "error_file": "s3://error_bucket/error_key/file_name.parquet.gzip",
                        "error_messages": [
                            {"ID": 0, "original_text": "cześć", "error_message": "Test"}
                        ],
                    },
                    "writing_status": "OK",
                },
            ],
            [
                "translate_destination_write_failure",
                [None, Exception("Test")],
                [
                    mock.call(
                        path="s3://destination_bucket/destination_key/file_name.parquet.gzip",
                        compression="gzip",
                    )
                ],
                {
                    "bucket": "destination_bucket",
                    "key": "destination_key",
                    "translation_status": "OK",
                    "writing_status": "Errors occured",
                    "writing_error_messages": "Test",
                },
            ],
        ]
    )
    @mock.patch(
        "translation_lambda.extract_path",
        return_value=["source_bucket", "source_key", "file_name"],
    )
    @mock.patch(
        "translation_lambda.pd.read_parquet",
        return_value=[
            [0, {"ID": 0, "review": "cześć"}],
            [1, {"ID": 1, "review": "pa"}],
        ],
    )
    @mock.patch(
        "translation_lambda.translate_dataframe",
        return_value=[
            [
                {
                    "ID": 0,
                    "original_review_language": "pl",
                    "review_translation": "hello",
                }
            ],
            [{"ID": 0, "original_text": "cześć", "error_message": "Test"}],
        ],
    )
    @mock.patch("translation_lambda.pd.DataFrame.to_parquet")
    @mock.patch("translation_lambda.DESTINATION_BUCKET_NAME", "destination_bucket")
    @mock.patch("translation_lambda.DESTINATION_LOCATION_PREFIX", "destination_key")
    @mock.patch("translation_lambda.ERROR_BUCKET_NAME", "error_bucket")
    @mock.patch("translation_lambda.ERROR_LOCATION_PREFIX", "error_key")
    def test_translate(
        self,
        name,
        pandas_write_outputs,
        expected_calls,
        expected_return_message,
        extract_path,
        pandas_read_parquet,
        translate_dataframe,
        pandas_dataframe_to_parquet,
    ):
        # GIVEN:
        record = "Some event string"
        translation_lambda.pd.DataFrame.to_parquet = mock.Mock(
            side_effect=pandas_write_outputs
        )
        # WHEN:
        response = translation_lambda.translate(record)
        # THEN:
        translation_lambda.extract_path.assert_called_once_with(
            record="Some event string"
        )
        translation_lambda.pd.read_parquet.assert_called_once_with(
            "s3://source_bucket/source_key"
        )
        translation_lambda.pd.DataFrame.to_parquet.assert_has_calls(
            expected_calls, any_order=True
        )
        self.assertDictEqual(response, expected_return_message)

    @mock.patch("translation_lambda.extract_path", side_effect=Exception("Test"))
    def test_translate_extract_path_error(self, extract_path):
        # GIVEN:
        record = "Some event string"
        # WHEN:
        response = translation_lambda.translate(record)
        # THEN:
        translation_lambda.extract_path.assert_called_once_with(
            record="Some event string"
        )
        self.assertDictEqual(
            response, {"path_extraction_error": "Test", "record": '"Some event string"'}
        )

    @mock.patch(
        "translation_lambda.translate",
        return_value={
            "bucket": "destination_bucket",
            "key": "destination_key",
            "translation_status": "OK",
        },
    )
    def test_lambda_handler(self, translate):
        # GIVEN:
        event = [
            {"bucket": "aw-lmb-nlp-data-lake", "key": "data/review_data.parquet.gzip"}
        ]
        context = {"Sample": "Context"}
        # WHEN:
        response = translation_lambda.lambda_handler(event=event, context=context)

        # THEN:
        self.assertListEqual(
            response,
            [
                {
                    "bucket": "destination_bucket",
                    "key": "destination_key",
                    "translation_status": "OK",
                }
            ],
        )
        translation_lambda.translate.assert_called_with(record=event[0])

    def test_lambda_handler_error_in_event(self):
        # GIVEN:
        event = None
        context = {"Sample": "Context"}
        # THEN:
        with self.assertRaises(Exception) as exception_context_manager:
            # WHEN:
            translation_lambda.lambda_handler(event=event, context=context)
            self.assertEqual(exception_context_manager.exception, Exception("bucket"))

    @mock.patch(
        "translation_lambda.pd.read_parquet",
        return_value=[
            [0, {"ID": 0, "review": "cześć"}],
            [1, {"ID": 1, "review": "pa"}],
        ],
    )
    @mock.patch(
        "translation_lambda.translate_dataframe",
        return_value=[
            [
                {
                    "ID": 0,
                    "original_review_language": "pl",
                    "review_translation": "hello",
                }
            ],
            [{"ID": 0, "original_text": "cześć", "error_message": "Test"}],
        ],
    )
    @mock.patch("translation_lambda.pd.DataFrame.to_parquet")
    @mock.patch("translation_lambda.DESTINATION_BUCKET_NAME", "destination_bucket")
    @mock.patch("translation_lambda.DESTINATION_LOCATION_PREFIX", "destination_key")
    @mock.patch("translation_lambda.ERROR_BUCKET_NAME", "error_bucket")
    @mock.patch("translation_lambda.ERROR_LOCATION_PREFIX", "error_key")
    def test_event_flow(
        self,
        pandas_read_parquet,
        translate_dataframe,
        pandas_dataframe_to_parquet,
    ):
        # GIVEN:
        event = [
            {"bucket": "aw-lmb-nlp-data-lake", "key": "data/review_data.parquet.gzip"}
        ]
        context = {"Sample": "Context"}
        bucket_name = event[0]["bucket"]
        key_name = event[0]["key"]

        # WHEN:
        response = translation_lambda.lambda_handler(event=event, context=context)
        # THEN:
        translation_lambda.pd.read_parquet.assert_called_once_with(
            f"s3://{bucket_name}/{key_name}"
        )

        calls = [
            mock.call(
                path="s3://destination_bucket/destination_key/review_data.parquet.gzip",
                compression="gzip",
            ),
            mock.call(
                path="s3://error_bucket/error_key/review_data.parquet.gzip",
                compression="gzip",
            ),
        ]
        translation_lambda.pd.DataFrame.to_parquet.assert_has_calls(calls)

        self.assertListEqual(
            response,
            [
                {
                    "bucket": "destination_bucket",
                    "key": "destination_key",
                    "translation_status": "Errors occured",
                    "translation_error_messages": {
                        "error_file": "s3://error_bucket/error_key/review_data.parquet.gzip",
                        "error_messages": [
                            {"ID": 0, "original_text": "cześć", "error_message": "Test"}
                        ],
                    },
                    "writing_status": "OK",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
