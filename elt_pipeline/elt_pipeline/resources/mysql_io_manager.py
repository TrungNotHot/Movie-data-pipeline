import pandas as pd
from sqlalchemy import create_engine


class MySQLIOManager:
    def __init__(self, config):
        self._config = config
        self.engine = create_engine(
            "mysql+pymysql://admin:admin123@localhost:3306/brazillian_ecommerce?charset=utf8mb4")

    def extract_data(self, sql: str) -> pd.DataFrame:
        pd.DataFrame = pd.read_sql_query(sql, self.engine)
        return pd.DataFrame
