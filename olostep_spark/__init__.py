"""
olostep-spark: Apache Spark UDFs for web enrichment powered by Olostep.

Usage:
    from pyspark.sql import SparkSession
    from olostep_spark import register_olostep_udfs

    spark = SparkSession.builder.appName("app").getOrCreate()
    register_olostep_udfs(spark, api_key="YOUR_KEY")

    result = spark.sql(\"\"\"
        SELECT olostep_enrich(
            map('company_name', company_name),
            array('CEO name', 'founding year', 'company description')
        ) as enriched
        FROM companies
    \"\"\")
"""

from olostep_spark.spark import register_olostep_udfs

__version__ = "0.1.0"
__all__ = ["register_olostep_udfs"]
