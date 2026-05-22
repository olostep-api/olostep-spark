"""Integration tests for olostep-spark (requires real API keys)."""

import json
import pytest


@pytest.mark.integration
def test_enrich_with_real_api(spark, olostep_api_key):
    """Test enrichment with real API key."""
    from olostep_spark import register_olostep_udfs

    register_olostep_udfs(
        spark,
        api_key=olostep_api_key,
        enrich_udf_name="real_enrich",
        search_udf_name="real_search",
    )

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW companies AS
        SELECT 'Google' as company_name
        UNION ALL
        SELECT 'Apple' as company_name
    """)

    result = spark.sql("""
        SELECT 
            company_name,
            real_enrich(
                map('company_name', company_name),
                array('CEO name', 'founding year')
            ) as enriched
        FROM companies
    """).collect()

    assert len(result) == 2
    for row in result:
        enriched = json.loads(row["enriched"])
        # Check that we got a response (could be error or data)
        assert isinstance(enriched, dict)
        # Should have at least the fields we requested or an error
        assert any(k in enriched for k in ['ceo_name', 'founding_year', 'error'])


@pytest.mark.integration
def test_search_with_real_api(spark, olostep_api_key):
    """Test search with real API key."""
    from olostep_spark import register_olostep_udfs

    register_olostep_udfs(
        spark,
        api_key=olostep_api_key,
        enrich_udf_name="real_enrich_s",
        search_udf_name="real_search",
    )

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW queries AS
        SELECT 'Google CEO' as query
        UNION ALL
        SELECT 'Apple stock price' as query
    """)

    result = spark.sql("""
        SELECT 
            query,
            real_search(query) as results
        FROM queries
    """).collect()

    assert len(result) == 2
    for row in result:
        results = json.loads(row["results"])
        # Check that we got a response (could be error or data array)
        assert isinstance(results, (list, dict))
