FROM python:3.9.17-slim
# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/dagster_home
WORKDIR $DAGSTER_HOME
COPY requirements.txt $DAGSTER_HOME
RUN pip install -r requirements.txt