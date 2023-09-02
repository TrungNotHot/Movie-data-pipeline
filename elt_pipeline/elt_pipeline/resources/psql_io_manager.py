import pandas as pd
from sqlalchemy import create_engine
from dagster import IOManager, OutputContext


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.engine = create_engine(
            "postgresql+psycopg2://admin:admin123@localhost:5432/postgres"
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        obj.to_sql(context.asset_key.path[-1], self.engine, if_exists='replace', index=False)
    # có thể làm vầy nếu muốn tới schema khác
    # schema_name = 'gold'  # Change this to the desired schema name
    # table_name = context.asset_key.path[-1]
    # full_table_name = f'{schema_name}.{table_name}'
    def load_input(self):
        pass