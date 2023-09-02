import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.minio_client = Minio(
            self._config["endpoint_url"],
            access_key=self._config["aws_access_key_id"],
            secret_key=self._config["aws_secret_access_key"],
            secure=False
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        bucket_name = self._config["bucket"]
        file_name = context.asset_key.path
        obj.to_csv(f"/tmp/{file_name[-1]}.csv", index=False)

        self.minio_client.fput_object(
            bucket_name, f"{file_name[-3]}/{file_name[-2]}/{file_name[-1]}.csv", f"/tmp/{file_name[-1]}.csv",
        )

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        bucket_name = self._config["bucket"]
        file_name = context.upstream_output.asset_key.path
        data = self.minio_client.get_object(bucket_name, f"{file_name[-3]}/{file_name[-2]}/{file_name[-1]}.csv")
        df = pd.read_csv(data)
        return df
