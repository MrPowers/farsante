import pytest
import pyspark.sql.functions as F
import farsante
from mimesis import Person
from mimesis import Address
from mimesis import Datetime


def test_create_fake_csv():
    person = Person()
    address = Address()
    datetime = Datetime()
    df = farsante.pandas_df([person.full_name, person.email, address.city, address.state, datetime.datetime], 3)
    df.to_csv('./tmp/fake_data.csv', index=False)
    # print(df)


def test_create_fake_parquet(spark):
    person = Person()
    address = Address()
    datetime = Datetime()
    df = farsante.pandas_df([person.full_name, person.email, address.city, address.state, datetime.datetime], 3)
    df.to_parquet('./tmp/fake_data.parquet', index=False)
    s_df = spark.read.parquet('./tmp/fake_data.parquet')
    # s_df.show()
