# Databricks notebook source

# MAGIC %md
# MAGIC # Atlas Stream — Country Pipeline Demo
# MAGIC
# MAGIC Full **Medallion ETL pipeline** walkthrough, step by step:
# MAGIC
# MAGIC | Step | Module | Flow |
# MAGIC |------|--------|------|
# MAGIC | 1 | `ingest` | REST Countries API → `bronze.countries_raw` |
# MAGIC | 2 | `bronze_to_silver` | `bronze.countries_raw` → `silver.countries` |
# MAGIC | 3 | `silver_to_gold` | `silver.countries` → 6 × `gold.*` tables |
# MAGIC | 4 | `verify` | Quality checks across all 8 tables |
# MAGIC
# MAGIC Set the **catalog** widget below and hit *Run All*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration & Imports

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql.functions import col, length

print(f"Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC  REST Countries API (v3.1)
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌────────────────────────────┐
# MAGIC │  BRONZE · countries_raw    │  raw JSON · append-only · immutable
# MAGIC └────────────────────────────┘
# MAGIC          │  parse + schema enforcement
# MAGIC          ▼
# MAGIC ┌────────────────────────────┐
# MAGIC │  SILVER · countries        │  structured · one row per country (keyed by cca2)
# MAGIC └────────────────────────────┘
# MAGIC          │  aggregate + enrich
# MAGIC          ▼
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  GOLD · countries              (enriched, with density) │
# MAGIC │       · countries_by_region                            │
# MAGIC │       · countries_by_continent                         │
# MAGIC │       · population_tiers                               │
# MAGIC │       · landlocked_vs_coastal                          │
# MAGIC │       · un_membership_summary                          │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1 — Ingest (API → Bronze)

# COMMAND ----------

from atlas_stream.etl.ingest import ingest_countries

SOURCE_API_URL = (
    "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,population,area,landlocked,unMember"
)

displayHTML("""
<div style="background:#FF6900;color:#fff;padding:16px 24px;border-radius:8px;font-family:sans-serif;">
  <h2 style="margin:0 0 6px 0;">🔄 Step 1 — Ingest</h2>
  <p style="margin:0;opacity:.9;">
    Fetching all countries from the REST Countries API and landing each raw JSON payload
    as a single row into <strong>bronze.countries_raw</strong>.<br/>
    Bronze is <em>append-only and immutable</em> — every run adds a new batch
    identified by <code>batch_id</code> and <code>ingested_at</code>.
  </p>
</div>
""")

ingest_countries(CATALOG, SOURCE_API_URL)
print("✓ Ingest complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze table — latest batch

# COMMAND ----------

bronze_df = spark.read.table(f"`{CATALOG}`.bronze.countries_raw")

latest_batch = bronze_df.orderBy(col("ingested_at").desc()).select("batch_id", "ingested_at", "source_url").limit(1)
displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>Latest Batch Metadata</h4>")
display(latest_batch)

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>Sample Raw Rows (with payload size in bytes)</h4>")
display(
    bronze_df.withColumn("payload_bytes", length(col("raw_json")))
    .select("ingested_at", "batch_id", "payload_bytes", "raw_json")
    .orderBy(col("ingested_at").desc())
    .limit(5)
)

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>All Batches in Bronze (newest first)</h4>")
display(
    bronze_df.groupBy("batch_id", "ingested_at")
    .count()
    .withColumnRenamed("count", "row_count")
    .orderBy(col("ingested_at").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2 — Bronze → Silver

# COMMAND ----------

from atlas_stream.etl.bronze_to_silver import bronze_to_silver

displayHTML("""
<div style="background:#0058A3;color:#fff;padding:16px 24px;border-radius:8px;font-family:sans-serif;">
  <h2 style="margin:0 0 6px 0;">🔧 Step 2 — Bronze → Silver</h2>
  <p style="margin:0;opacity:.9;">
    Parsing raw JSON with an <em>explicit StructType schema</em> — no fragile single-row inference.
    Filters to the most recent batch, applies a <strong>cca2 null-drop safety check</strong>
    (&lt;2% max), and writes one clean row per country to <strong>silver.countries</strong>.
  </p>
</div>
""")

bronze_to_silver(CATALOG)
print("✓ Bronze → Silver complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver table — key metrics

# COMMAND ----------

silver_df = spark.read.table(f"`{CATALOG}`.silver.countries")

total = silver_df.count()
regions = silver_df.select("region").distinct().count()
un_members = silver_df.filter(col("un_member") == True).count()
landlocked = silver_df.filter(col("landlocked") == True).count()

displayHTML(f"""
<div style="font-family:sans-serif;">
  <h4 style="margin:0 0 12px 0;">Silver Layer — At a Glance</h4>
  <div style="display:flex;gap:12px;flex-wrap:wrap;">
    <div style="background:#e3f0fb;border:1px solid #0058A3;border-radius:8px;padding:16px 24px;text-align:center;min-width:120px;">
      <div style="font-size:2em;font-weight:bold;color:#0058A3;">{total}</div>
      <div style="color:#555;margin-top:4px;">Countries</div>
    </div>
    <div style="background:#e3f0fb;border:1px solid #0058A3;border-radius:8px;padding:16px 24px;text-align:center;min-width:120px;">
      <div style="font-size:2em;font-weight:bold;color:#0058A3;">{regions}</div>
      <div style="color:#555;margin-top:4px;">Regions</div>
    </div>
    <div style="background:#e8f5e9;border:1px solid #00843D;border-radius:8px;padding:16px 24px;text-align:center;min-width:120px;">
      <div style="font-size:2em;font-weight:bold;color:#00843D;">{un_members}</div>
      <div style="color:#555;margin-top:4px;">UN Members</div>
    </div>
    <div style="background:#fff3e0;border:1px solid #FF6900;border-radius:8px;padding:16px 24px;text-align:center;min-width:120px;">
      <div style="font-size:2em;font-weight:bold;color:#FF6900;">{landlocked}</div>
      <div style="color:#555;margin-top:4px;">Landlocked</div>
    </div>
  </div>
</div>
""")

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>All Countries (sorted alphabetically)</h4>")
display(silver_df.orderBy("name_common"))

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>Country Count per Region</h4>")
display(
    silver_df.groupBy("region").count().withColumnRenamed("count", "country_count").orderBy(col("country_count").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3 — Silver → Gold

# COMMAND ----------

from atlas_stream.etl.silver_to_gold import silver_to_gold

displayHTML("""
<div style="background:#FFD200;color:#111;padding:16px 24px;border-radius:8px;font-family:sans-serif;">
  <h2 style="margin:0 0 6px 0;">✨ Step 3 — Silver → Gold</h2>
  <p style="margin:0;opacity:.8;">
    Building <strong>6 analytical Gold tables</strong> from structured Silver data.
    Each table is optimised for a specific query pattern:
    enrichment, regional rollups, population buckets, landlocked analysis, and UN membership.
  </p>
</div>
""")

silver_to_gold(CATALOG)
print("✓ Silver → Gold complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.countries — enriched country-level table

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>Top 20 Countries by Population</h4>")
display(
    spark.read.table(f"`{CATALOG}`.gold.countries")
    .select(
        "name_common",
        "cca2",
        "region",
        "population",
        "area",
        "population_density",
        "size_category",
        "landlocked",
        "un_member",
    )
    .orderBy(col("population").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.countries_by_continent

# COMMAND ----------

display(
    spark.read.table(f"`{CATALOG}`.gold.countries_by_continent")
    .select("continent", "country_count", "total_population", "total_area_km2", "un_member_count")
    .orderBy(col("total_population").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.countries_by_region

# COMMAND ----------

display(
    spark.read.table(f"`{CATALOG}`.gold.countries_by_region")
    .select("region", "subregion", "country_count", "total_population", "total_area_km2", "un_member_count")
    .orderBy("region", col("total_population").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.population_tiers

# COMMAND ----------

display(
    spark.read.table(f"`{CATALOG}`.gold.population_tiers")
    .select("population_tier", "country_count", "total_population")
    .orderBy(col("total_population").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.landlocked_vs_coastal

# COMMAND ----------

display(
    spark.read.table(f"`{CATALOG}`.gold.landlocked_vs_coastal")
    .select("region", "landlocked", "country_count", "total_population")
    .orderBy("region", "landlocked")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.un_membership_summary

# COMMAND ----------

display(
    spark.read.table(f"`{CATALOG}`.gold.un_membership_summary")
    .select("region", "un_member", "country_count", "total_population")
    .orderBy("region", "un_member")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4 — Data Quality Verification

# COMMAND ----------

from atlas_stream.etl.verify import verify_country_tables

displayHTML("""
<div style="background:#00843D;color:#fff;padding:16px 24px;border-radius:8px;font-family:sans-serif;">
  <h2 style="margin:0 0 6px 0;">✅ Step 4 — Verify</h2>
  <p style="margin:0;opacity:.9;">
    Running <strong>7 data quality checks</strong> across all 8 Medallion tables:<br/>
    non-empty tables · silver row count [220–260] · cca2 uniqueness ·
    region null rate &lt;5% · no null/negative population · gold↔silver reconciliation.
  </p>
</div>
""")

try:
    verify_country_tables(CATALOG)
    displayHTML("""
    <div style="background:#e8f5e9;border:2px solid #00843D;border-radius:8px;padding:16px 24px;font-family:sans-serif;margin-top:12px;">
      <h3 style="margin:0 0 4px 0;color:#00843D;">✅ All 7 quality checks passed!</h3>
      <p style="margin:0;color:#333;">
        All 8 tables are non-empty. Row counts are within the expected range.
        No duplicate cca2 values, no null or negative populations, and the Gold totals reconcile with Silver.
      </p>
    </div>
    """)
except AssertionError as e:
    displayHTML(f"""
    <div style="background:#ffebee;border:2px solid #c62828;border-radius:8px;padding:16px 24px;font-family:sans-serif;margin-top:12px;">
      <h3 style="margin:0 0 8px 0;color:#c62828;">❌ Quality checks failed</h3>
      <pre style="margin:0;color:#333;white-space:pre-wrap;background:#fff;padding:12px;border-radius:4px;">{e}</pre>
    </div>
    """)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🌍 Pipeline Summary

# COMMAND ----------

gold_countries = spark.read.table(f"`{CATALOG}`.gold.countries")
gold_continent = spark.read.table(f"`{CATALOG}`.gold.countries_by_continent")

total_countries = gold_countries.count()
total_population = gold_countries.agg({"population": "sum"}).collect()[0][0]
num_continents = gold_continent.count()
largest_country = gold_countries.orderBy(col("area").desc()).select("name_common", "area").first()
most_populous = gold_countries.orderBy(col("population").desc()).select("name_common", "population").first()
densest_country = (
    gold_countries.filter(col("population_density").isNotNull())
    .orderBy(col("population_density").desc())
    .select("name_common", "population_density")
    .first()
)


def _fmt(n):
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


displayHTML(f"""
<div style="font-family:sans-serif;padding:8px 0;">
  <h2 style="margin:0 0 16px 0;">🌍 Pipeline Run Complete</h2>
  <div style="display:flex;gap:14px;flex-wrap:wrap;">
    <div style="background:#fff3e0;border:1px solid #FF6900;border-radius:8px;padding:20px 28px;text-align:center;min-width:130px;">
      <div style="font-size:2.4em;font-weight:bold;color:#FF6900;">{total_countries}</div>
      <div style="color:#666;margin-top:4px;">Total Countries</div>
    </div>
    <div style="background:#e3f0fb;border:1px solid #0058A3;border-radius:8px;padding:20px 28px;text-align:center;min-width:130px;">
      <div style="font-size:2.4em;font-weight:bold;color:#0058A3;">{_fmt(total_population)}</div>
      <div style="color:#666;margin-top:4px;">World Population</div>
    </div>
    <div style="background:#f3e5f5;border:1px solid #7B1FA2;border-radius:8px;padding:20px 28px;text-align:center;min-width:130px;">
      <div style="font-size:2.4em;font-weight:bold;color:#7B1FA2;">{num_continents}</div>
      <div style="color:#666;margin-top:4px;">Continents</div>
    </div>
    <div style="background:#e8f5e9;border:1px solid #00843D;border-radius:8px;padding:20px 28px;text-align:center;min-width:140px;">
      <div style="font-size:1.3em;font-weight:bold;color:#00843D;">{largest_country["name_common"]}</div>
      <div style="color:#666;margin-top:4px;">Largest by Area</div>
      <div style="color:#999;font-size:.85em;">{_fmt(int(largest_country["area"]))} km²</div>
    </div>
    <div style="background:#fff8e1;border:1px solid #F57F17;border-radius:8px;padding:20px 28px;text-align:center;min-width:140px;">
      <div style="font-size:1.3em;font-weight:bold;color:#F57F17;">{most_populous["name_common"]}</div>
      <div style="color:#666;margin-top:4px;">Most Populous</div>
      <div style="color:#999;font-size:.85em;">{_fmt(int(most_populous["population"]))} people</div>
    </div>
    <div style="background:#fce4ec;border:1px solid #c2185b;border-radius:8px;padding:20px 28px;text-align:center;min-width:140px;">
      <div style="font-size:1.3em;font-weight:bold;color:#c2185b;">{densest_country["name_common"]}</div>
      <div style="color:#666;margin-top:4px;">Densest Country</div>
      <div style="color:#999;font-size:.85em;">{_fmt(int(densest_country["population_density"]))} people/km²</div>
    </div>
  </div>
</div>
""")

# COMMAND ----------

displayHTML("<h4 style='font-family:sans-serif;margin:8px 0;'>Population &amp; Country Count by Continent</h4>")
display(
    gold_continent.select(
        "continent", "country_count", "total_population", "total_area_km2", "un_member_count"
    ).orderBy(col("total_population").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *Atlas Stream Country Pipeline · Medallion Architecture (Bronze → Silver → Gold) · REST Countries API v3.1*
