# olostep-spark

Apache Spark UDFs for web enrichment powered by [Olostep](https://www.olostep.com).

Enrich any dataset with live web intelligence directly in PySpark SQL queries —
no custom API integrations, no leaving your pipeline.

Inspired by [parallel-web-tools](https://docs.parallel.ai/data-integrations/spark).

## Installation

```bash
pip install olostep-spark[spark]
```

## Quick Start

```python
from pyspark.sql import SparkSession
from olostep_spark import register_olostep_udfs

spark = SparkSession.builder.appName("app").getOrCreate()
register_olostep_udfs(spark, api_key="YOUR_OLOSTEP_API_KEY")

result = spark.sql("""
    SELECT
        company_name,
        olostep_enrich(
            map('company_name', company_name),
            array('CEO name', 'founding year', 'company description')
        ) as enriched
    FROM companies
""")

result.show(truncate=False)
```

Get your key at https://www.olostep.com/dashboard

## UDFs

| UDF | Input | Output |
|-----|-------|--------|
| `olostep_enrich(input_data, output_columns)` | `map<string,string>`, `array<string>` | JSON object |
| `olostep_search(query)` | query string | JSON array of `{url, title, description}` |

## Parsing Results

```python
from pyspark.sql.functions import get_json_object, col

# Extract individual fields
result.withColumn("ceo", get_json_object(col("enriched"), "$.ceo_name")).show()

# Filter out errors
clean = result.filter(get_json_object(col("enriched"), "$.error").isNull())
```

## Partition Sizing

All rows in a partition are processed concurrently. For best performance:

```python
# Aim for 20 rows per partition
df.repartition(df.count() // 20).createOrReplaceTempView("my_table")
```

## Configuration

```python
register_olostep_udfs(
    spark,
    api_key="your-key",          # or set OLOSTEP_API_KEY env var
    timeout=60,                   # per-request timeout in seconds
    enrich_udf_name="olostep_enrich",  # customize SQL function name
    search_udf_name="olostep_search",
)
```

## Demo

See [notebooks/spark_enrichment_demo.ipynb](notebooks/spark_enrichment_demo.ipynb)

## License

MIT — © 2025 Olostep

## Testing

### Unit Tests (No API Key Required)

```bash
pytest tests/test_spark.py
```

Tests validate UDF registration, error handling, and edge cases using mocked responses.

### Integration Tests (Requires API Key)

1. Set your Olostep API key:
```bash
export OLOSTEP_API_KEY="your-api-key"
```

2. Run integration tests:
```bash
pytest tests/test_integration.py -m integration -v
```

Integration tests exercise the UDFs against the real Olostep API to validate end-to-end functionality.
