import pytest
import pyspark.sql.functions as F
import farsante
from mimesis import Person


def test_quick_pandas_df(spark):
    df = farsante.quick_pandas_df(['first_name', 'last_name'], 3)
    # print(df)
    assert len(df.index) == 3


def test_pandas_df(spark):
    mx = Person('es-mx')
    df = farsante.pandas_df([mx.first_name, mx.last_name], 5)
    # print(df)
    assert len(df.index) == 5


