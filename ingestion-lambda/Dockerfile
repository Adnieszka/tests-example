FROM public.ecr.aws/lambda/python:3.8
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY ingestion_lambda.py  .
RUN chmod +xr ingestion_lambda.py
CMD [ "ingestion_lambda.lambda_handler"]
