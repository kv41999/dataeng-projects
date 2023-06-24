import os
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from custom.operators import MovielensFetchRatingsOperator
from custom.sensors import MovielensRatingsSensor
from airflow import DAG

with DAG(
    dag_id="04_sensor",
    start_date=datetime(2019,1,1),
    schedule_interval="@daily"
) as dag:
    wait_for_ratings = MovielensRatingsSensor(
        task_id = "wait_for_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}"
    )

    fetch_ratings = MovielensFetchRatingsOperator(
    task_id = "fetch_ratings",
    conn_id="movielens",
    start_date="{{ds}}",
    end_date="{{next_ds}}",
    output_path=f"C:/bigdata/airflow/ratings/{{ds}}.json"
    )

    def _rank_movies_by_rating(ratings, min_rating = 2):
        ranking = (ratings.groupby("movieId")
                .agg(
                        avg_rating = pd.NamedAgg(column="rating", aggfunc="mean"),
                        num_rating = pd.NamedAgg(column="userId", aggfunc="nunique")
                )
                .loc[lambda df: df["num_rating"] > min_rating]
                .sort_values(["avg_rating", "num_rating"], ascending = False))
        return ranking

    def _rank_movies(templates_dict, min_rating = 2, **_):
        input_path = templates_dict["input_path"]
        output_path = templates_dict["output_path"]

        ratings = pd.read_json(input_path)
        ranking = _rank_movies_by_rating(ratings=ratings, min_rating=min_rating)

        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        ranking.to_csv(output_path, index=True)

    rank_movies = PythonOperator(
        task_id = "rank_movies",
        python_callable=_rank_movies,
        templates_dict={
            "input_path" : "C:/bigdata/airflow/ratings/{{ds}}.json",
            "output_path": "C:/bigdata/airflow/rankings/{{ds}}.json"
        }
    )   

    wait_for_ratings>>fetch_ratings>>rank_movies