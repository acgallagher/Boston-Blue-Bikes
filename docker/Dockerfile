FROM prefecthq/prefect:2.8.5-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY prefect /opt/prefect/flows
COPY data opt/prefect/data