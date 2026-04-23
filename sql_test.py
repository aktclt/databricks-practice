import os
import sys

if sys.version_info < (3, 11):
    raise SystemExit(
        "Use Python 3.11+ (.venv311). Example: source .venv311/bin/activate"
    )

from databricks.connect import DatabricksSession

_builder = DatabricksSession.builder
if hasattr(_builder, "serverless"):
    spark = _builder.serverless(True).getOrCreate()
else:
    os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    spark = _builder.getOrCreate()

spark.sql("SELECT current_user() AS user, current_timestamp() AS ts").show(truncate=False)