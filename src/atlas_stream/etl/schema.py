"""Explicit PySpark schema for a single REST Countries API v3.1 country object.

Using an explicit StructType instead of schema_of_json avoids:
- Silent field omissions when the first sampled row lacks an optional field
- Type ambiguity (e.g., integer vs null) from single-row inference

Bump SCHEMA_VERSION and extend COUNTRY_ROW_SCHEMA when the API adds new fields.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SCHEMA_VERSION = 1

# Schema for a single country object as returned by the REST Countries API v3.1
# with ?fields=name,cca2,cca3,region,subregion,population,area,landlocked,unMember
COUNTRY_ROW_SCHEMA = StructType([
    StructField(
        "name",
        StructType([
            StructField("common", StringType(), nullable=True),
            StructField("official", StringType(), nullable=True),
        ]),
        nullable=True,
    ),
    StructField("cca2", StringType(), nullable=True),
    StructField("cca3", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("subregion", StringType(), nullable=True),
    StructField("population", LongType(), nullable=True),
    StructField("area", DoubleType(), nullable=True),
    StructField("landlocked", BooleanType(), nullable=True),
    StructField("unMember", BooleanType(), nullable=True),
])

# Set of top-level field names required by bronze_to_silver.py
REQUIRED_API_FIELDS: frozenset[str] = frozenset(COUNTRY_ROW_SCHEMA.fieldNames())
