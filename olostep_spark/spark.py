"""
Olostep Spark UDFs.

Registers SQL-native UDFs for web data enrichment directly in PySpark
pipelines. Mirrors the design of parallel-web-tools for easy migration.

Primary UDF:
    olostep_enrich(input_data, output_columns) -> STRING (JSON)

    input_data:    map<string, string> of context key-value pairs
    output_columns: array<string> of natural language field descriptions

    Returns JSON object where each output_column becomes a snake_case key.
    Failed rows return: {"error": "..."}

Secondary UDF:
    olostep_search(query) -> STRING (JSON array of links)

Usage:
    from olostep_spark import register_olostep_udfs
    register_olostep_udfs(spark, api_key="YOUR_KEY")

    spark.sql(\"\"\"
        SELECT
            company_name,
            get_json_object(
                olostep_enrich(
                    map('company_name', company_name),
                    array('CEO name', 'founding year')
                ),
                '$.ceo_name'
            ) as ceo
        FROM companies
    \"\"\")
"""

import asyncio
import json
import logging
import os
import re
from typing import Optional

import requests
from olostep import AsyncOlostep
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def _to_snake_case(text: str) -> str:
    """Convert 'CEO name' -> 'ceo_name', 'Founding Year' -> 'founding_year'."""
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", "_", text.strip())
    return text.lower()


def register_olostep_udfs(
    spark,
    api_key: Optional[str] = None,
    timeout: int = 60,
    enrich_udf_name: str = "olostep_enrich",
    search_udf_name: str = "olostep_search",
) -> None:
    """
    Register Olostep UDFs with a SparkSession.

    After calling this, use olostep_enrich() and olostep_search() in any
    Spark SQL query.

    Args:
        spark:            Active SparkSession.
        api_key:          Olostep API key. Falls back to OLOSTEP_API_KEY env var.
        timeout:          Per-request timeout in seconds (default: 60).
        enrich_udf_name:  SQL name for the enrichment UDF (default: olostep_enrich).
        search_udf_name:  SQL name for the search UDF (default: olostep_search).

    Raises:
        ValueError:  If no API key is found.
        ImportError: If pyspark or pandas are not installed.

    Example:
        >>> register_olostep_udfs(spark, api_key="sk-...")
        >>> spark.sql(\"\"\"
        ...     SELECT olostep_enrich(
        ...         map('company', name),
        ...         array('CEO name', 'founding year')
        ...     ) as data FROM companies
        ... \"\"\")
    """
    resolved_key = api_key or os.environ.get("OLOSTEP_API_KEY", "")
    if not resolved_key:
        raise ValueError(
            "Olostep API key not found. "
            "Pass api_key= or set the OLOSTEP_API_KEY environment variable. "
            "Get a key at https://www.olostep.com/dashboard"
        )

    try:
        import pandas as pd
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import MapType, ArrayType, StringType
    except ImportError as exc:
        raise ImportError(
            "PySpark and pandas are required. "
            "Install with: pip install olostep-spark[spark]"
        ) from exc

    # ------------------------------------------------------------------ #
    # UDF 1: olostep_enrich                                                #
    # Pandas UDF. Receives entire partition as two Series:                 #
    #   - input_data: Series of dicts (map<string,string>)                 #
    #   - output_columns: Series of lists (array<string>)                  #
    # All rows in the partition are processed concurrently via asyncio.    #
    #                                                                      #
    # For each row, builds a task string from input_data and output_columns#
    # then calls client.answers.create(task=...) from the Olostep SDK.    #
    # Returns JSON string per row.                                         #
    # ------------------------------------------------------------------ #

    @pandas_udf(StringType())
    def _enrich(
        input_data_series,
        output_columns_series,
    ):
        import pandas as pd
        from olostep import AsyncOlostep
        from olostep.errors import Olostep_BaseError, OlostepServerError_AuthFailed

        async def _enrich_one(
            client,
            input_data,
            output_columns,
        ):
            """Enrich a single row using Olostep answers API."""
            try:
                # Build a structured task from input context + desired outputs
                context_str = ", ".join(
                    f"{k}: {v}" for k, v in input_data.items()
                )
                fields_str = ", ".join(output_columns)
                task = (
                    f"Given this information: {context_str}\n"
                    f"Please provide the following as a JSON object with "
                    f"snake_case keys: {fields_str}\n"
                    f"Respond ONLY with valid JSON, no explanation."
                )

                result = await client.answers.create(task=task)
                raw = result.answer.strip()

                # Strip markdown code fences if present
                if raw.startswith("```"):
                    raw = re.sub(r"```(?:json)?\n?", "", raw).strip()
                    raw = raw.rstrip("`").strip()

                # Validate it's parseable JSON
                parsed = json.loads(raw)

                # Normalize keys to snake_case to match output_columns
                normalized = {}
                for col in output_columns:
                    snake = _to_snake_case(col)
                    # Find the key in parsed (case-insensitive fallback)
                    if snake in parsed:
                        normalized[snake] = parsed[snake]
                    else:
                        # Try to find a matching key
                        for k, v in parsed.items():
                            if _to_snake_case(k) == snake:
                                normalized[snake] = v
                                break
                        else:
                            normalized[snake] = None

                return json.dumps(normalized)

            except OlostepServerError_AuthFailed:
                return json.dumps({"error": "Invalid Olostep API key"})
            except json.JSONDecodeError as exc:
                logger.warning("JSON parse failed: %s — raw: %s", exc, raw[:200])
                return json.dumps({"error": f"JSON parse error: {exc}"})
            except Olostep_BaseError as exc:
                logger.warning("Olostep error: %s", exc)
                return json.dumps({"error": str(exc)})
            except Exception as exc:
                logger.warning("Unexpected error: %s", exc)
                return json.dumps({"error": str(exc)})

        async def _enrich_all(rows):
            async with AsyncOlostep(api_key=resolved_key) as client:
                tasks = [
                    _enrich_one(
                        client,
                        row[0] if isinstance(row[0], dict) and row[0] else {},
                        list(row[1]) if hasattr(row[1], '__iter__') and len(row[1]) > 0 else []
                    )
                    for row in rows
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
            return [
                r if isinstance(r, str) else json.dumps({"error": str(r)})
                for r in results
            ]

        rows = list(zip(input_data_series.tolist(), output_columns_series.tolist()))
        return pd.Series(asyncio.run(_enrich_all(rows)))

    spark.udf.register(enrich_udf_name, _enrich)
    logger.info("Registered Olostep UDF: %s", enrich_udf_name)

    # ------------------------------------------------------------------ #
    # UDF 2: olostep_search                                                #
    # Pandas UDF. Receives a partition of query strings.                   #
    # Calls /v1/searches via requests for each row.                        #
    # Returns JSON array: [{url, title, description}, ...]                 #
    # ------------------------------------------------------------------ #

    @pandas_udf(StringType())
    def _search(queries):
        def _search_one(query: str) -> str:
            if not query or not query.strip():
                return json.dumps([])
            try:
                resp = requests.post(
                    "https://api.olostep.com/v1/searches",
                    headers={
                        "Authorization": f"Bearer {resolved_key}",
                        "Content-Type": "application/json",
                    },
                    json={"query": query.strip()},
                    timeout=timeout,
                )
                resp.raise_for_status()
                links = resp.json().get("result", {}).get("links", [])
                return json.dumps(links)
            except Exception as exc:
                logger.warning("olostep_search error for '%s': %s", query, exc)
                return json.dumps({"error": str(exc)})

        return queries.apply(_search_one)

    spark.udf.register(search_udf_name, _search)
    logger.info("Registered Olostep UDF: %s", search_udf_name)

    logger.info(
        "Olostep UDFs registered successfully.\n"
        "  %s(map(...), array(...)) -> JSON enrichment\n"
        "  %s(query_string)         -> JSON search results",
        enrich_udf_name,
        search_udf_name,
    )
