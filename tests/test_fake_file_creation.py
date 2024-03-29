import pytest
import pyspark.sql.functions as F
import farsante
from mimesis import Person
from mimesis import Address
from mimesis import Datetime
from pathlib import Path


def ensure_tmp_exists():
    Path("./tmp/").mkdir(parents=True, exist_ok=True)


def test_create_fake_csv():
    ensure_tmp_exists()
    person = Person()
    address = Address()
    datetime = Datetime()
    df = farsante.pandas_df(
        [
            person.full_name,
            person.email,
            address.city,
            address.state,
            datetime.datetime,
        ],
        3,
    )
    df.to_csv("./tmp/fake_data.csv", index=False)
    # print(df)


def test_create_fake_parquet(spark):
    ensure_tmp_exists()
    person = Person()
    address = Address()
    datetime = Datetime()
    df = farsante.pandas_df(
        [
            person.full_name,
            person.email,
            address.city,
            address.state,
            datetime.datetime,
        ],
        3,
    )
    df.to_parquet("./tmp/fake_data.parquet", index=False)
    s_df = spark.read.parquet("./tmp/fake_data.parquet")
    # s_df.show()


def test_create_fake_parquet_pyspark(spark):
    ensure_tmp_exists()
    person = Person()
    address = Address()
    datetime = Datetime()
    df = farsante.pyspark_df(
        [
            person.full_name,
            person.email,
            address.city,
            address.state,
            datetime.datetime,
        ],
        3,
    )
    df.write.mode("overwrite").parquet("./tmp/spark_fake_data")
    # s_df.show()
