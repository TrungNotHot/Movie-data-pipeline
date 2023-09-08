from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from dagster import Definitions
from .assets.bronze_layer import (
    bronze_movies,
    bronze_keywords,
    bronze_links,
    bronze_credits,
    bronze_ratings,
)
from .assets.silver_layer import (
    silver_cleaned_movies,
    silver_cleaned_keywords,
    silver_cleaned_links,
    silver_cleaned_credits,
    silver_cleaned_ratings,
)
from .assets.warehouse_layer import (
    warehouse_credits,
    warehouse_keywords,
    warehouse_links,
    warehouse_movies,
    warehouse_ratings,
)

MYSQL_CONFIG = {
    # "host": "localhost",
    "host": "de_mysql",
    "port": 3306,
    "database": "movies_db",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    # "endpoint_url": "localhost:9000",
    "endpoint_url": "minio:9000",
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
        bronze_credits,
        bronze_ratings,
        silver_cleaned_movies,
        silver_cleaned_keywords,
        silver_cleaned_links,
        silver_cleaned_credits,
        silver_cleaned_ratings,
        warehouse_credits,
        warehouse_keywords,
        warehouse_links,
        warehouse_movies,
        warehouse_ratings,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    }
)
