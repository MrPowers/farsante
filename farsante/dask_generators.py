from farsante.pandas_generators import *
import dask.dataframe as dd

def quick_dask_df(cols, num_rows, npartitions=1):
    pandas_df = quick_pandas_df(cols, num_rows)
    return dd.from_pandas(pandas_df, npartitions=npartitions)

