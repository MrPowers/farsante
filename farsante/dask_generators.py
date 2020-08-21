from farsante.pandas_generators import *
import dask.dataframe as dd

def quick_dask_df(cols, num_rows, npartitions=1):
    df = quick_pandas_df(cols, num_rows)
    return dd.from_pandas(df, npartitions=npartitions)


def dask_df(funs, num_rows, npartitions=1):
    df = pandas_df(funs, num_rows)
    return dd.from_pandas(df, npartitions=npartitions)

