from datetime import datetime
import pandas as pd
from dagster import asset, Output, StaticPartitionsDefinition

id_partition = StaticPartitionsDefinition(
    ["0_10000"] + [f"{i * 10000 + 1}_{(i + 1) * 10000}" for i in range(1, 5)]
)
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1995, datetime.today().year)]
)


# movies metadata
@asset(
    name="bronze_movies",
    description="Load table movie from mysql and load to minio",
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'raw'],
    compute_kind='MySQL',
    group_name='bronze',
)
def bronze_movies(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM movie;"
    movie_dt = context.resources.mysql_io_manager.extract_data(query)
    return Output(
        movie_dt,
        metadata={
            "table": "movie",
            "row_count": movie_dt.shape[0],
            "column_count": movie_dt.shape[1],
        },
    )


# keywords
@asset(
    name="bronze_keywords",
    description="Load table keywords from mysql and load to minio",
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'raw'],
    compute_kind='MySQL',
    group_name='bronze',
)
def bronze_keywords(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM keyword;"
    keywords_dt = context.resources.mysql_io_manager.extract_data(query)

    return Output(
        keywords_dt,
        metadata={
            "table": "keywords",
            "row_count": keywords_dt.shape[0],
            "column_count": keywords_dt.shape[1],
        },
    )


# links
@asset(
    name="bronze_links",
    description="Load table links from mysql and load to minio",
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'raw'],
    compute_kind='MySQL',
    group_name='bronze',
)
def bronze_links(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM link;"
    links_dt = context.resources.mysql_io_manager.extract_data(query)
    return Output(
        links_dt,
        metadata={
            "table": "links",
            "row_count": links_dt.shape[0],
            "column_count": links_dt.shape[1],
        },
    )


# credits
@asset(
    name="bronze_credits",
    description="Load table credits from mysql and load to minio",
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'raw'],
    compute_kind='MySQL',
    group_name='bronze',
    partitions_def=id_partition,
)
def bronze_credits(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM credit"
    try:
        partition = context.asset_partition_key_for_output()
        partition_by = "id"
        ranges = partition.split('_')
        query += f" WHERE {partition_by} BETWEEN {ranges[0]} AND {ranges[1]}"
        context.log.info(f"Partition by {partition_by} = {partition}")
    except Exception:
        context.log.info("No partition key found")
    credits_dt = context.resources.mysql_io_manager.extract_data(query)

    return Output(
        credits_dt,
        metadata={
            "table": "credits",
            "row_count": credits_dt.shape[0],
            "column_count": credits_dt.shape[1],
        },
    )

# ratings
@asset(
    name="bronze_ratings",
    description="Load table ratings from mysql and load to minio",
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'raw'],
    compute_kind='MySQL',
    group_name='bronze',
    partitions_def=YEARLY,
)
def bronze_ratings(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM rating"
    try:
        partition = context.asset_partition_key_for_output()
        partition_by = "timestamp"
        query += f" WHERE YEAR({partition_by}) = {partition}"
        context.log.info(f"Partition by {partition_by} = {partition}")
    except Exception:
        context.log.info("No partition key found")
    ratings_dt = context.resources.mysql_io_manager.extract_data(query)

    return Output(
        ratings_dt,
        metadata={
            "table": "ratings",
            "row_count": ratings_dt.shape[0],
            "column_count": ratings_dt.shape[1],
        },
    )
