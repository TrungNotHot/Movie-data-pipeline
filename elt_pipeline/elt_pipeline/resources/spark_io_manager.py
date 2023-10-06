import os
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from pyspark.sql import SparkSession
from contextlib import contextmanager
from datetime import datetime
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq


def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists.")


@contextmanager
def get_spark_session(run_id="movies_benchmark"):
    try:
        spark = (
            SparkSession.builder.appName(run_id)
            .master("spark://spark-master:7077")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.minio_client = Minio(
            self._config["endpoint_url"],
            access_key=self._config["aws_access_key_id"],
            secret_key=self._config["aws_secret_access_key"],
            secure=False,
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            "-".join(context.asset_key.path),
            datetime.today().strftime("%Y%m%d%H%M%S"),
        )
        if context.has_partition_key:
            partition_str = str(table) + "_" + context.asset_partition_key
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)
        try:
            bucket_name = self._config["bucket"]
            make_bucket(self.minio_client, bucket_name)
            self.minio_client.fput_object(
                bucket_name, key_name, tmp_file_path,
            )
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: InputContext) -> pd.DataFrame:
        bucket_name = self._config["bucket"]
        key_name, tmp_file_path = self._get_path(context)
        try:
            make_bucket(self.minio_client, bucket_name)
            self.minio_client.fget_object(
                bucket_name, key_name, tmp_file_path,
            )
            df = pd.read_parquet(tmp_file_path)
            # Clean up tmp file
            os.remove(tmp_file_path)
            return df
        except Exception as e:
            raise e
