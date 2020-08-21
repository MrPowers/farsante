import pytest
import farsante

import random
from mimesis import Person
from mimesis import Address
from mimesis import Datetime


def test_quick_dask_df():
    df = farsante.quick_dask_df(['first_name', 'last_name'], 3)
    # print(df.head(3))
    assert len(df.index) == 3


def test_dask_df():
    def rand_int(min_int, max_int):
        def f():
            return random.randint(min_int, max_int)
        return f
    mx = Person('es-mx')
    df = farsante.dask_df([mx.first_name, mx.last_name, rand_int(1000, 2000)], 5)
    # print(df.head(3))
    assert len(df.index) == 5
