from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
# from resources.psql_io_manager import PostgreSQLIOManager
from dagster import Definitions
from assets.bronze_layer import (
    bronze_movies,
    bronze_keywords,
    bronze_links,
)
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "movies_db",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}


defs = Definitions(
    assets=[
        bronze_movies,
        bronze_keywords,
        bronze_links,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        # "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    }
)