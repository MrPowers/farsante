import pandas as pd

def people_df():
    return pd.read_csv('./data/people.csv')


def iris_df():
    return pd.read_csv('./data/iris.csv')
