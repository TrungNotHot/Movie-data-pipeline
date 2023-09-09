from datetime import datetime
from dagster import asset, Output, AssetIn, StaticPartitionsDefinition
import pandas as pd

id_partition = StaticPartitionsDefinition(
    ["0_10000"] + [f"{i * 10000 + 1}_{(i + 1) * 10000}" for i in range(1, 5)]
)
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1995, datetime.today().year)]
)


@asset(
    description="Load credits from silver to warehouse after cleaning",
    io_manager_key='psql_io_manager',
    key_prefix=['warehouse', 'movies_db'],
    compute_kind='Postgres',
    group_name='warehouse',
    ins={
        "silver_cleaned_credits": AssetIn(
            key_prefix=['silver', 'movies_db'],
        ),
    },
    partitions_def=id_partition,
)
def warehouse_credits(context, silver_cleaned_credits: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_cleaned_credits,
        metadata={
            "schema": "movies_db",
            "table": "credits",
            "row_count": silver_cleaned_credits.shape[0],
            "column_count": silver_cleaned_credits.shape[1],
        }
    )


@asset(
    description="Load keywords from silver to warehouse after cleaning",
    io_manager_key='psql_io_manager',
    key_prefix=['warehouse', 'movies_db'],
    compute_kind='Postgres',
    group_name='warehouse',
    ins={
        "silver_cleaned_keywords": AssetIn(
            key_prefix=['silver', 'movies_db'],
        )
    }
)
def warehouse_keywords(context, silver_cleaned_keywords: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_cleaned_keywords,
        metadata={
            "schema": "movies_db",
            "table": "keywords",
            "row_count": silver_cleaned_keywords.shape[0],
            "column_count": silver_cleaned_keywords.shape[1],
        }
    )


@asset(
    description="Load links from silver to warehouse after cleaning",
    io_manager_key='psql_io_manager',
    key_prefix=['warehouse', 'movies_db'],
    compute_kind='Postgres',
    group_name='warehouse',
    ins={
        "silver_cleaned_links": AssetIn(
            key_prefix=['silver', 'movies_db'],
        )
    }
)
def warehouse_links(context, silver_cleaned_links: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_cleaned_links,
        metadata={
            "schema": "movies_db",
            "table": "links",
            "row_count": silver_cleaned_links.shape[0],
            "column_count": silver_cleaned_links.shape[1],
        }
    )


@asset(
    description="Load movies from silver to warehouse after cleaning",
    io_manager_key='psql_io_manager',
    key_prefix=['warehouse', 'movies_db'],
    compute_kind='Postgres',
    group_name='warehouse',
    ins={
        "silver_cleaned_movies": AssetIn(
            key_prefix=['silver', 'movies_db'],
        )
    }
)
def warehouse_movies(context, silver_cleaned_movies: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_cleaned_movies,
        metadata={
            "schema": "movies_db",
            "table": "movies",
            "row_count": silver_cleaned_movies.shape[0],
            "column_count": silver_cleaned_movies.shape[1],
        }
    )


@asset(
    description="Load ratings from silver to warehouse after cleaning",
    io_manager_key='psql_io_manager',
    key_prefix=['warehouse', 'movies_db'],
    compute_kind='Postgres',
    group_name='warehouse',
    ins={
        "silver_cleaned_ratings": AssetIn(
            key_prefix=['silver', 'movies_db'],
        )
    },
    partitions_def=YEARLY,
)
def warehouse_ratings(context, silver_cleaned_ratings: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_cleaned_ratings,
        metadata={
            "schema": "movies_db",
            "table": "ratings",
            "row_count": silver_cleaned_ratings.shape[0],
            "column_count": silver_cleaned_ratings.shape[1],
        }
    )
