import delta_sharing
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

profile_file = "/home/nguynmanh/works/recommender/RestaurantWeb/key/delta-sharing.share"
# Create a Delta Sharing client
client = delta_sharing.SharingClient(profile=profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#bronze-layer.bronze.business"

# Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data from a table that cannot fit in the memory.
print("########### Loading 10 rows from delta_sharing.default.owid-covid-data as a Pandas DataFrame #############")
data = delta_sharing.load_as_pandas(table_url, limit=10)

# Print the sample.
print("########### Show the fetched 10 rows #############")
print(data)