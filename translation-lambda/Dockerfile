FROM public.ecr.aws/lambda/python:3.8
COPY requirements.txt .
RUN pip3 install -r requirements.txt
ADD translation_lambda.py .
RUN chmod +xr translation_lambda.py
CMD [ "translation_lambda.lambda_handler"]