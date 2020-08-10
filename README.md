# farsante

Fake Pandas / PySpark DataFrame creator.

## Install

`pip install farsante`

## PySpark

Here's how to quickly create a 7 row DataFrame with `fake_first_name` and `fake_last_name` fields.

```python
import farsante

df = farsante.quick_pyspark_df(['first_name', 'last_name'], 7)
df.show()
```

```

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

```

## Pandas



