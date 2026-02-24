FROM python:3.13-slim
RUN pip install --no-cache-dir pyarrow boto3
COPY inspect_arrow.py /usr/local/bin/
ENTRYPOINT ["python", "/usr/local/bin/inspect_arrow.py"]
