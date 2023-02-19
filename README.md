# farsante

Fake Pandas / PySpark DataFrame creator.

## Install

`pip install farsante`

## PySpark

Here's how to quickly create a 7 row DataFrame with `first_name` and `last_name` fields.

```python
import farsante

df = farsante.quick_pyspark_df(['first_name', 'last_name'], 7)
df.show()
```

```
+----------+---------+
|first_name|last_name|
+----------+---------+
|     Tommy|     Hess|
|    Arthur| Melendez|
|  Clemente|    Blair|
|    Wesley|   Conrad|
|    Willis|   Dunlap|
|     Bruna|  Sellers|
|     Tonda| Schwartz|
+----------+---------+
```

Here's how to create a DataFrame with 5 rows of data with first names and last names using Mexican Spanish.

```python
import farsante
from mimesis import Person

mx = Person('es-mx')

df = farsante.pyspark_df([mx.first_name, mx.last_name], 5)
df.show()
```

```
+-----------+---------+
| first_name|last_name|
+-----------+---------+
|     Connie|    Xicoy|
|  Oliverios|   Merino|
|     Castel|    Yáñez|
|Guillelmina|   Prieto|
|     Gezane|   Campos|
+-----------+---------+
```

## Pandas

Here's how to quickly create a 3 row DataFrame with `first_name` and `last_name` fields.

```python
import farsante

df = farsante.quick_pandas_df(['first_name', 'last_name'], 3)
print(df)
```

```
  first_name last_name
0       Toby   Rosales
1      Gregg    Hughes
2    Terence       Ray
```

Here's how to create a 5 row DataFrame with first names and last names using Russian.

```python
from mimesis import Person
ru = Person('ru')
df = farsante.pandas_df([ru.first_name, ru.last_name], 5)
print(df)
```

```
  first_name   last_name
0      Амиль  Ханженкова
1  Славентий  Голумидова
2    Паладин   Волосиков
3       Акша    Бабашова
4       Ника    Синусова
```

## Fake files

Here's how to create a CSV file with some fake data:

```python
import farsante
from mimesis import Person
from mimesis import Address
from mimesis import Datetime

person = Person()
address = Address()
datetime = Datetime()
df = farsante.pandas_df([person.full_name, person.email, address.city, address.state, datetime.datetime], 3)
df.to_csv('./tmp/fake_data.csv', index=False)
```

Here's how to create a Parquet file with fake data:

```python
df = farsante.pandas_df([person.full_name, person.email, address.city, address.state, datetime.datetime], 3)
df.to_parquet('./tmp/fake_data.parquet', index=False)
```

