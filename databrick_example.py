import os
import sys

if sys.version_info < (3, 11):
    raise SystemExit(
        "Databricks Connect + serverless needs Python 3.11+. "
        "Use: source .venv311/bin/activate  "
        "or in Jupyter pick kernel …/databricks-practice/.venv311/bin/python"
    )

from databricks.connect import DatabricksSession

# Initialize the session (auth from ~/.databrickscfg or env; use DEFAULT profile if set).
_builder = DatabricksSession.builder
if hasattr(_builder, "serverless"):
    spark = _builder.serverless(True).getOrCreate()
else:
    os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    spark = _builder.getOrCreate()

# Read a table from Unity Catalog or Hive Metastore
df = spark.read.table("samples.nyctaxi.trips")

# Perform standard PySpark transformations
df_filtered = df.filter(df.trip_distance > 10).limit(5)

# Execute remotely and show results locally
df_filtered.show(5)

