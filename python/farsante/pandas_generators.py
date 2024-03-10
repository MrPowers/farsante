import pandas as pd
from mimesis import Person

def quick_pandas_df(cols, num_rows):
    en = Person('en')
    data = []
    for i in range(num_rows):
        stuff = list(map(lambda col: getattr(en, col)(), cols))
        data.append(stuff)
    df = pd.DataFrame(data, columns = cols)
    return df


def pandas_df(funs, num_rows):
    data = []
    for i in range(num_rows):
        stuff = list(map(lambda f: f(), funs))
        data.append(stuff)
    col_names = list(map(lambda f: f.__name__, funs))
    df = pd.DataFrame(data, columns = col_names)
    return df

