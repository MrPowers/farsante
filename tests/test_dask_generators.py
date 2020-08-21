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
