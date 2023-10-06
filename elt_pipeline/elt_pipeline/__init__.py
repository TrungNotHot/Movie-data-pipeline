from dagster_dbt import DbtCliClientResource, dbt_cli_resource, load_assets_from_dbt_project
from .resources.spark_io_manager import SparkIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from dagster import Definitions, load_assets_from_modules, file_relative_path
from . import assets

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
DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt_transform")
DBT_PROFILES = file_relative_path(__file__, "../dbt_transform/config")

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    "spark_io_manager": SparkIOManager(MINIO_CONFIG),
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        }
    ),
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources,
)
