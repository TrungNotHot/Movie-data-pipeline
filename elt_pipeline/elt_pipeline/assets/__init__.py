from dagster import load_assets_from_modules, file_relative_path
from dagster_dbt import load_assets_from_dbt_project

from . import bronze_layer, silver_layer, warehouse_layer

bronze_layer_assets = load_assets_from_modules([bronze_layer])
silver_layer_assets = load_assets_from_modules([silver_layer])
warehouse_assets = load_assets_from_modules([warehouse_layer])

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_transform")
DBT_PROFILES = file_relative_path(__file__, "../../dbt_transform/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
)
