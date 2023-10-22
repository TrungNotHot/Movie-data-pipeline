from datetime import datetime
from dagster import asset, Output, StaticPartitionsDefinition, AssetIn
import pandas as pd

id_partition = StaticPartitionsDefinition(
    ["0_10000"] + [f"{i * 10000 + 1}_{(i + 1) * 10000}" for i in range(1, 5)]
)
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1995, datetime.today().year)]
)


@asset(
    description="Load credits from bronze to silver and clean data",
    io_manager_key='minio_io_manager',
    key_prefix=['silver', 'movies_db'],
    compute_kind='Pandas',
    group_name='silver',
    ins={
        "bronze_credits": AssetIn(
            key_prefix=['bronze', 'movies_db'],
        )
    },
    partitions_def=id_partition,
)
def silver_cleaned_credits(context, bronze_credits: pd.DataFrame) -> Output[pd.DataFrame]:
    credits_df = bronze_credits.drop_duplicates()
    credits_df = credits_df[credits_df["cast"].notnull() & credits_df["crew"].notnull()]
    return Output(
        credits_df,
        metadata={
            "table": "cleaned credits",
            "row_count": credits_df.shape[0],
            "column_count": credits_df.shape[1],
        }
    )


@asset(
    description="Load ratings from bronze to silver and clean data",
    io_manager_key='minio_io_manager',
    key_prefix=['silver', 'movies_db'],
    compute_kind='Pandas',
    group_name='silver',
    ins={
        "bronze_ratings": AssetIn(
            key_prefix=['bronze', 'movies_db'],
        )
    },
    partitions_def=YEARLY,
)
def silver_cleaned_ratings(context, bronze_ratings: pd.DataFrame) -> Output[pd.DataFrame]:
    ratings_df = bronze_ratings.drop_duplicates()
    ratings_df = ratings_df[ratings_df["rating"].notnull()]
    ratings_df = ratings_df[ratings_df["timestamp"].notnull()]
    ratings_df = ratings_df[(ratings_df["rating"] >= 0) & (ratings_df["rating"] <= 5)]

    return Output(
        ratings_df,
        metadata={
            "table": "cleaned ratings",
            "row_count": ratings_df.shape[0],
            "column_count": ratings_df.shape[1],
        }
    )


@asset(
    description="Load keywords from bronze to silver and clean data",
    io_manager_key='minio_io_manager',
    key_prefix=['silver', 'movies_db'],
    compute_kind='Pandas',
    group_name='silver',
    ins={
        "bronze_keywords": AssetIn(
            key_prefix=['bronze', 'movies_db'],
        )
    }
)
def silver_cleaned_keywords(context, bronze_keywords: pd.DataFrame) -> Output[pd.DataFrame]:
    keywords_df = bronze_keywords.drop_duplicates()
    keywords_df = keywords_df[keywords_df["keywords"].notnull() & keywords_df["keywords"] != "[]"]
    return Output(
        keywords_df,
        metadata={
            "table": "cleaned keywords",
            "row_count": keywords_df.shape[0],
            "column_count": keywords_df.shape[1],
        }
    )


@asset(
    description="Load links from bronze to silver and clean data",
    io_manager_key='minio_io_manager',
    key_prefix=['silver', 'movies_db'],
    compute_kind='Pandas',
    group_name='silver',
    ins={
        "bronze_links": AssetIn(
            key_prefix=['bronze', 'movies_db'],
        )
    }
)
def silver_cleaned_links(context, bronze_links: pd.DataFrame) -> Output[pd.DataFrame]:
    links_df = bronze_links.drop_duplicates()
    links_df = links_df[links_df["imdbId"].notnull() & links_df["tmdbId"].notnull()]
    return Output(
        links_df,
        metadata={
            "table": "cleaned links",
            "row_count": links_df.shape[0],
            "column_count": links_df.shape[1],
        }
    )


@asset(
    description="Load movies from bronze to silver and clean data",
    io_manager_key='minio_io_manager',
    key_prefix=['silver', 'movies_db'],
    compute_kind='Pandas',
    group_name='silver',
    ins={
        "bronze_movies": AssetIn(
            key_prefix=['bronze', 'movies_db'],
        )
    }
)
def silver_cleaned_movies(context, bronze_movies: pd.DataFrame) -> Output[pd.DataFrame]:
    movies_df = bronze_movies.drop_duplicates()
    movies_df = movies_df[movies_df["budget"].notnull()]
    movies_df = movies_df[movies_df["genres"].notnull() & movies_df["genres"] != "[]"]
    movies_df = movies_df[movies_df["imdb_id"].notnull()]
    movies_df = movies_df[movies_df["original_language"].notnull()]
    movies_df = movies_df[movies_df["original_title"].notnull() & movies_df["original_title"] != ""]
    movies_df = movies_df[movies_df["popularity"].notnull()]
    movies_df = movies_df[movies_df["production_companies"].notnull()]
    movies_df = movies_df[movies_df["release_date"].notnull()]
    movies_df = movies_df[movies_df["revenue"].notnull()]
    movies_df = movies_df[movies_df["status"].notnull()]
    movies_df = movies_df[movies_df["title"].notnull()]
    movies_df = movies_df[
        (movies_df["vote_average"].notnull()) &
        (movies_df["vote_average"] >= 0) &
        (movies_df["vote_average"] <= 10)
        ]
    movies_df = movies_df[movies_df["vote_count"].notnull() & (movies_df["vote_count"] >= 0)]

    return Output(
        movies_df,
        metadata={
            "table": "cleaned movies",
            "row_count": movies_df.shape[0],
            "column_count": movies_df.shape[1],
        }
    )
