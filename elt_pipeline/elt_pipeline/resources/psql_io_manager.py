import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from dagster import IOManager, OutputContext, InputContext


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.engine = create_engine(
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            + f"@{config['host']}:{config['port']}"
            + f"/{config['database']}"
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        layer, schema, table = context.asset_key.path
        temp=obj.drop(obj.index, inplace=False)
        if context.has_partition_key:
            try:
                obj.to_sql(
                    name=f"{table.replace(f'{layer}_', '')}",
                    con=self.engine,
                    schema=f"{schema}",
                    if_exists='append',
                    index=False
                )
            except Exception:
                context.log.info(f"Table {table} already exists. Replacing it.")
                temp.to_sql(
                    name=f"{table.replace(f'{layer}_', '')}",
                    con=self.engine,
                    schema=f"{schema}",
                    if_exists='replace',
                    index=False
                )
        else:
            obj.to_sql(
                name=f"{table.replace(f'{layer}_', '')}",
                con=self.engine,
                schema=f"{schema}",
                if_exists='replace',
                index=False
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass
