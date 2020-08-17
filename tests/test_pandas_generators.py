import pytest
import pyspark.sql.functions as F
import random
import farsante
from mimesis import Person
from mimesis import Address
from mimesis import Datetime


def test_quick_pandas_df():
    df = farsante.quick_pandas_df(['first_name', 'last_name'], 3)
    # print(df)
    assert len(df.index) == 3


def test_pandas_df():
    def rand_int(min_int, max_int):
        def f():
            return random.randint(min_int, max_int)
        return f
    mx = Person('es-mx')
    df = farsante.pandas_df([mx.first_name, mx.last_name, rand_int(1000, 2000)], 5)
    # print(df)
    assert len(df.index) == 5


def test_pandas_df_from_so():
    person = Person()
    address = Address()
    datetime = Datetime()
    def rand_int(min_int, max_int):
        def some_rand_int():
            return random.randint(min_int, max_int)
        return some_rand_int
    df = farsante.pandas_df([
        person.full_name,
        address.address,
        person.name,
        person.email,
        address.city,
        address.state,
        datetime.datetime,
        rand_int(1000, 2000)], 5)
    print('')
    print(df)

