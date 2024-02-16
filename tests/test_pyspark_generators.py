import pytest
import pyspark.sql.functions as F
import farsante
from mimesis import Person
import random


def test_quick_pyspark_df():
    df = farsante.quick_pyspark_df(["first_name", "last_name"], 7, spark=None)
    # df.show()
    assert df.count() == 7


def test_pyspark_df():
    mx = Person("es-mx")
    df = farsante.pyspark_df([mx.first_name, mx.last_name], 5)
    # df.show()
    assert df.count() == 5


def test_pyspark_custom_function():
    person = Person()

    def rand_int(min_int, max_int):
        def some_rand_int():
            return random.randint(min_int, max_int)

        return some_rand_int

    df = farsante.pyspark_df([person.full_name, rand_int(1000, 2000)], 5)
    df.show()
