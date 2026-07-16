dbutils.widgets.text("catalog_name", "transportation", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

# Create Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schemas under the catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold;")
