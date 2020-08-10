import pytest
import pyspark.sql.functions as F
import farsante
from mimesis import Person

def test_quick_pyspark_df(spark):
    df = farsante.quick_pyspark_df(['first_name', 'last_name'], 7)
    # df.show()
    assert df.count() == 7


def test_pyspark_df(spark):
    mx = Person('es-mx')
    df = farsante.pyspark_df([mx.first_name, mx.last_name], 5)
    # df.show()
    assert df.count() == 5

