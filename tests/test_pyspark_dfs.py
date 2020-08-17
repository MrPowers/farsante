import pytest
from farsante.pyspark_dfs import *


def test_people_df():
    df = people_df()
    assert df.count() == 100

