import pandas as pd
from dagster import asset, Output


# movies metadata
@asset(
    name="bronze_movies",
    description="Load table movie from mysql and load to minio",
    # io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'movies_db'],
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
            "column_names": movie_dt.columns,
        },
    )


# keywords
@asset(
    name="bronze_keywords",
    description="Load table keywords from mysql and load to minio",
    # io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'movies_db'],
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
            "column_names": keywords_dt.columns,
        },
    )


# links
@asset(
    name="bronze_links",
    description="Load table links from mysql and load to minio",
    # io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'movies_db'],
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
            "column_names": links_dt.columns,
        },
    )
# ratings
# credits
