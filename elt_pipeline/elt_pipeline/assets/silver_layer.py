from datetime import datetime
from dagster import asset, Output, StaticPartitionsDefinition, AssetIn
import pandas as pd
from pyspark.sql import SparkSession
from ..resources.spark_io_manager import get_spark_session

id_partition = StaticPartitionsDefinition(
    ["0_10000"] + [f"{i * 10000 + 1}_{(i + 1) * 10000}" for i in range(1, 5)]
)
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1995, datetime.today().year)]
)


@asset(
    description="Load credits from bronze to silver and clean data",
    # io_manager_key='spark_io_manager',
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
    # with get_spark_session() as spark:
    #     credits_df = spark.createDataFrame(bronze_credits)
    #     credits_df.cache()
    #     credits_df = credits_df.dropDuplicates()
    #     credits_df = credits_df.filter(credits_df["cast"].isNotNull() & credits_df["crew"].isNotNull())
    #     credits_df = credits_df.toPandas()
    #     credits_df.unpersist()
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
    # with get_spark_session() as spark:
    #     ratings_df = spark.createDataFrame(bronze_ratings)
    #     ratings_df.cache()
    #     ratings_df = ratings_df.dropDuplicates()
    #     ratings_df = ratings_df.na.drop(subset=["rating"])
    #     ratings_df = ratings_df.na.drop(subset=["timestamp"])
    #     ratings_df = ratings_df.filter(ratings_df["rating"] >= 0 & ratings_df["rating"] <= 5)
    #     ratings_df = ratings_df.toPandas()
    #     ratings_df.unpersist()
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
    io_manager_key='spark_io_manager',
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
    spark = (SparkSession.builder.appName("pyspark-dataframe-demo-{}".format(datetime.today()))
    .master("spark://spark-master:7077")      
    .getOrCreate())
    keywords_df = spark.createDataFrame(bronze_keywords)
    keywords_df.cache()
    keywords_df = keywords_df.dropDuplicates()
    keywords_df = keywords_df.filter(keywords_df["keywords"].isNotNull()
                                        & keywords_df["keywords"] != "[]"
                                        )
    keywords_df = keywords_df.toPandas()
    keywords_df.unpersist()
    # keywords_df = bronze_keywords.drop_duplicates()
    # keywords_df = keywords_df[keywords_df["keywords"].notnull() & keywords_df["keywords"] != "[]"]

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
    # with get_spark_session() as spark:
    #     links_df = spark.createDataFrame(bronze_links)
    #     links_df.cache()
    #     links_df = links_df.dropDuplicates()
    #     links_df = links_df.filter(links_df["imdbId"].isNotNull()
    #                                & links_df["tmdbId"].isNotNull()
    #                                )
    #     links_df = links_df.toPandas()
    #     links_df.unpersist()
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
    # with get_spark_session() as spark:
    #     movies_df = spark.createDataFrame(bronze_movies)
    #     movies_df.cache()
    #     movies_df = movies_df.dropDuplicates()
    #     movies_df = movies_df.filter(movies_df["budget"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["genres"].isNotNull()
    #                                  & movies_df["genres"] != "[]"
    #                                  )
    #     movies_df = movies_df.filter(movies_df["imdb_id"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["original_language"].isNotNull()
    #                                  & length(movies_df["original_language"]) == 2
    #                                  )
    #     movies_df = movies_df.filter(movies_df["original_title"].isNotNull()
    #                                  & movies_df["original_title"] != ""
    #                                  )
    #     movies_df = movies_df.filter(movies_df["popularity"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["production_companies"].isNotNull()
    #                                  & movies_df["production_companies"] != "[]")
    #     movies_df = movies_df.filter(movies_df["release_date"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["revenue"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["spoken_languages"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["status"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["title"].isNotNull())
    #     movies_df = movies_df.filter(movies_df["vote_average"].isNotNull()
    #                                  & movies_df["vote_average"] >= 0
    #                                  & movies_df["vote_average"] <= 5)
    #     movies_df = movies_df.filter(movies_df["vote_count"].isNotNull())
    #     movies_df = movies_df.toPandas()
    #     movies_df.unpersist()
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
