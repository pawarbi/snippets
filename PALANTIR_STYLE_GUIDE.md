# PySpark Style Guide

> **Purpose**: This notebook is a style contract for AI-assisted code generation and review.
> When referenced from another notebook (e.g., `refer to @pyspark_style_guide`),
> you MUST apply every rule below to all PySpark code produced or reviewed in that session.
>
> Adapted from [Palantir PySpark Style Guide](https://github.com/palantir/pyspark-style-guide) (MIT License).

---

## VERSIONS

Use features and API supported by following versions:

- Spark 3.5
- Delta 3.2
- Python 3.11

## Enforcement Checklist

When reviewing or generating PySpark code, walk through each check below **in order**.
Flag every violation found. Do not skip checks.

| #  | Check | What to look for |
|----|-------|-----------------|
| C1 | **Imports** | Any bare `from pyspark.sql.functions import ...` or alias other than `F`, `T`, `W` |
| C2 | **Column access** | Any `df.colName` dot-access outside of a join `on=` clause |
| C3 | **String column refs** | Any `F.col('x')` that could just be `'x'` (Spark 3.0+) |
| C4 | **Variable names** | Any single-letter dataframe names (`df`, `o`, `d`, `t`) |
| C5 | **Magic values** | Any literal string, number, or threshold inline in `filter`, `when`, `withColumn`, `select` that is not a named constant |
| C6 | **Select contract** | More than one function per column in a `select`, or a `.when()` expression inside a `select` |
| C7 | **withColumnRenamed** | Any use. Replace with `select` + `.alias()` |
| C8 | **Empty columns** | Any `lit('')`, `lit('NA')`, `lit('N/A')`. Must be `lit(None)` |
| C9 | **Logical density** | More than 3 boolean expressions in a single `.filter()` or `F.when()` without named variables |
| C10 | **Chain length** | More than 5 chained statements in one block |
| C11 | **Chain mixing** | Joins, filters, withColumn, and selects mixed in the same chain |
| C12 | **Join hygiene** | Any `.join()` missing explicit `how=` |
| C13 | **Right joins** | Any `how='right'`. Swap df order, use `left` |
| C14 | **Window frames** | Any `Window.partitionBy(...).orderBy(...)` without explicit `.rowsBetween()` or `.rangeBetween()` |
| C15 | **Window nulls** | `F.first()` or `F.last()` without `ignorenulls=True` |
| C16 | **Global windows** | Empty `W.partitionBy()` or window without `orderBy` used for aggregation. Use `.agg()` instead |
| C17 | **Otherwise fallback** | `.otherwise(<catch-all value>)` masking unexpected data. Use `None` or omit |
| C18 | **Line continuation** | Any `\` for multiline. Wrap in parentheses instead |
| C19 | **UDFs** | Any `@udf` or `F.udf()`. Rewrite with native functions |
| C20 | **Comments** | Comments that describe *what* code does instead of *why* a decision was made |
| C21 | **Dead code** | Commented-out code blocks. Remove them |
| C22 | **Function size** | Functions over ~70 lines or files over ~250 lines |

---

## Anti-Patterns (find and fix these)

Each pattern below is a regex-like signature. If you see it, it is a violation.

### AP1: Bare function imports
```python
# VIOLATION: any of these
from pyspark.sql.functions import col, when, sum, lit
import pyspark.sql.functions as func

# FIX: always
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W
```

### AP2: Dot-access column references
```python
# VIOLATION: df.column_name anywhere except join on=
df.select(df.order_id, df.amount)
df.withColumn('x', df.price * df.qty)

# FIX: use string refs
df.select('order_id', 'amount')
df.withColumn('x', F.col('price') * F.col('qty'))
```

### AP3: Inline magic values
```python
# VIOLATION: bare literals in logic
df.filter(F.col('amount') > 500)
F.when(F.col('status') == 'shipped', 'In Transit')
df.filter(F.col('days') < 365)

# FIX: named constants at top of cell/function
HIGH_VALUE_THRESHOLD = 500
STATUS_SHIPPED = 'shipped'
LABEL_IN_TRANSIT = 'In Transit'
ONE_YEAR_DAYS = 365

df.filter(F.col('amount') > HIGH_VALUE_THRESHOLD)
F.when(F.col('status') == STATUS_SHIPPED, LABEL_IN_TRANSIT)
df.filter(F.col('days') < ONE_YEAR_DAYS)
```

### AP4: Complex logic inside .when() or .filter()
```python
# VIOLATION: more than 3 conditions inline
df.filter(
    (F.col('a') == 'x') & (F.col('b') > 10) & (F.col('c') != 'y')
    & ((F.col('d') == 'online') | (F.col('d') == 'partner'))
)

# FIX: named boolean expressions, max 3 in the final filter
is_valid_type = (F.col('a') == TYPE_X)
above_threshold = (F.col('b') > MIN_THRESHOLD)
not_excluded = (F.col('c') != EXCLUDED_STATUS)
is_target_channel = (F.col('d') == CHANNEL_ONLINE) | (F.col('d') == CHANNEL_PARTNER)

flagged = is_valid_type & above_threshold & not_excluded & is_target_channel
df.filter(flagged)
```

### AP5: .when() inside select
```python
# VIOLATION: conditional logic embedded in select
df.select(
    'order_id',
    F.when(F.col('status') == 'shipped', 'In Transit')
     .when(F.col('status') == 'delivered', 'Complete')
     .alias('status_label'),
)

# FIX: select plain columns, then withColumn for derived logic
df = df.select('order_id', 'status')
df = df.withColumn(
    'status_label',
    F.when(F.col('status') == STATUS_SHIPPED, LABEL_IN_TRANSIT)
     .when(F.col('status') == STATUS_DELIVERED, LABEL_COMPLETE)
)
```

### AP6: Empty column sentinels
```python
# VIOLATION
df.withColumn('notes', F.lit(''))
df.withColumn('review_date', F.lit('N/A'))

# FIX
df.withColumn('notes', F.lit(None))
df.withColumn('review_date', F.lit(None))
```

### AP7: Missing window frame
```python
# VIOLATION: implicit frame
w = W.partitionBy('customer_id').orderBy('order_date')

# FIX: always explicit
w = (W.partitionBy('customer_id')
      .orderBy('order_date')
      .rowsBetween(W.unboundedPreceding, 0))
```

### AP8: Blanket .otherwise()
```python
# VIOLATION: masks unexpected values
F.when(..., 'A').when(..., 'B').otherwise('Unknown')

# FIX: omit otherwise (returns null) or use lit(None) explicitly
F.when(..., 'A').when(..., 'B')
```

### AP9: Monster chains
```python
# VIOLATION: mixed concerns, too long
df = (df.select(...).filter(...).withColumn(...).join(...).drop(...).withColumn(...))

# FIX: separate by concern, max 5 per block
df = (
    df
    .select(...)
    .filter(...)
)
df = df.withColumn(...)
df = df.join(..., how='inner')
```

### AP10: Backslash continuation
```python
# VIOLATION
df = df.filter(F.col('a') == 'x') \
       .filter(F.col('b') > 10)

# FIX: parentheses
df = (
    df
    .filter(F.col('a') == 'x')
    .filter(F.col('b') > 10)
)
```

---

## Quick Reference (for code generation)

When **writing new code**, apply these defaults:

- Imports: `F`, `T`, `W` only
- Columns: string refs where possible, `F.col()` when needed
- Descriptive df names: `orders_df`, `active_orders`, not `df`, `o`
- Constants: every literal in logic gets a `SCREAMING_SNAKE` name
- Selects: plain columns + one transform each, no `.when()` inside
- Chains: max 5 lines, group by concern (filter/select, then enrich, then join)
- Joins: always `how=`, always `left` not `right`, alias for disambiguation
- Windows: always explicit frame, always `ignorenulls=True` on `first`/`last`
- Empty cols: `F.lit(None)`, never `lit('')` or `lit('NA')`
- No UDFs, no `.otherwise()` fallbacks, no `\` continuations
- Comments explain *why*, not *what*. No commented-out code.
