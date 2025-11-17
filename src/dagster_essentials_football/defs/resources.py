import dagster as dg
from dagster_duckdb import DuckDBResource

db_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)

@dg.definitions
def resources():
    return dg.Definitions(resources={"database": db_resource})