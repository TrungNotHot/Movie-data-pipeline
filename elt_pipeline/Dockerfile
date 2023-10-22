FROM python:3.9.17-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root 

WORKDIR /opt/dagster/app

# Add repository code

COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /opt/dagster/app
COPY . /opt/dagster/app/elt_pipeline
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]
