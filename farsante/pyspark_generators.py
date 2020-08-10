from pyspark.sql import SparkSession
from mimesis import Person
import itertools


def quick_pyspark_df(cols, num_rows, spark = SparkSession.builder.getOrCreate()):
    # valid_cols = ['first_name', 'last_name']
    # if not (set(cols) <= set(valid_cols)):
        # raise ValueError(f"The valid column values are '{valid_cols}'.  You tried to use these cols '{cols}'.")
    en = Person('en')
    def funs():
        return tuple(map(lambda col: getattr(en, col)(), cols))
    data = []
    for _ in itertools.repeat(None, num_rows):
        data.append(funs())
    return spark.createDataFrame(data, cols)


def pyspark_df(funs, num_rows, spark = SparkSession.builder.getOrCreate()):
    def functions():
        return tuple(map(lambda f: f(), funs))
    cols = list(map(lambda f: f.__name__, funs))
    data = []
    for _ in itertools.repeat(None, num_rows):
        data.append(functions())
    return spark.createDataFrame(data, cols)

