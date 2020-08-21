import dask.dataframe as dd


def people_df():
    return dd.read_csv('./data/people.csv')


def iris_df():
    return dd.read_csv('./data/iris.csv')
