import pytest
from farsante.dask_dfs import *


def test_people_df():
    df = people_df()
    assert len(df) == 100


def test_iris_df():
    df = iris_df()
    assert len(df) == 150


