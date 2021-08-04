import unittest
import json
import ingestion_lambda
from parameterized import parameterized


class TestLambdaFunction(unittest.TestCase):
    @parameterized.expand(
        ['/temp/sample.csv', '/temp/sample..csv', '/temp///sample.csv', 'sample.csv', 'sample.csv.gzip'])
    def test_create_file_name(self, name):
        file_out_name = ingestion_lambda.create_file_name(name)
        self.assertEqual(file_out_name, "sample")

    def test_read_variables(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "test-s3-bucket",
                        },
                        "object": {
                            "key": "data/sample.csv"
                        }
                    }
                }
            ]
        }

        bucket_in_name, key_in_name = ingestion_lambda.read_variables(event['Records'][0])
        self.assertEqual(bucket_in_name, event['Records'][0]['s3']['bucket']['name'])
        self.assertEqual(key_in_name, event['Records'][0]['s3']['object']['key'])

    def test_empty_file_name(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "test-s3-bucket"
                        },
                        "object": {
                        }
                    }
                }
            ]
        }

        self.assertRaises(KeyError, ingestion_lambda.read_variables, event['Records'][0])
