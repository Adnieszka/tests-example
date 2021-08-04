from setuptools import setup, find_packages

import site
import sys

site.addsitedir('./src')  # Always appends to end
print(sys.path)

setup(
    name="lambda_translation",
    version="0.1",
    author="Rafał Siwek",
    author_email="rsiwek@pgs-soft.com",
    description=("Glue Extraction job script with test"),
    packages=find_packages()
)
