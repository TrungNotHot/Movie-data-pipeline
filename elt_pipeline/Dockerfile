FROM python:3.9.17-slim

USER root

#RUN apt-get update --yes && \
#    apt-get install --yes --no-install-recommends \
#    "openjdk-17-jre-headless" \
#    ca-certificates-java && \
#    apt-get clean && rm -rf /var/lib/apt/lists/*

# Add repository code
WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
RUN pip install -r requirements.txt
COPY . /opt/dagster/app
# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]