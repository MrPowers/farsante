import pytest
import pyspark.sql.functions as F
import farsante

def test_quick_pyspark_df(spark):
    df = farsante.quick_pyspark_df(['first_name', 'last_name'], 7)
    assert df.count() == 7


