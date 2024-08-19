from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import types
import pyarrow.compute as pc
import pyarrow as pa
import polars as pl
import time
import os

# Som Notes:
# You need to have bigquery.readsessions.create permissions in Big Query to use this code.
# This is different from standard permissions for Big Query like Job Viewer, for example.
# THis isn't hitting the Big Query API, it is hitting the Big Query Storage API, which is different

# Start the timer
start_time = time.time()

# I suggest using this to keep your keys.json file with credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# Create a storage read client
client = bigquery_storage.BigQueryReadClient()

# Details for where the table is
project_id = ""
dataset_id = ""
table_id = ""

# f string all of it together
table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

# Create the read session and add an arrow table to the session
read_session = types.ReadSession()
read_session.table = table
read_session.data_format = types.DataFormat.ARROW

# Specify the columns you want to select
# Reducing column list speeds things up significantly with wide tables
read_session.read_options = types.ReadSession.TableReadOptions(
    selected_fields=["col1", "col2", "col3"],  # Replace with your desired column names
    row_restriction="column_name > 100" # Replace with row filter condition
)
requested_session.read_options.row_restriction = 'state = "WA"'

# Big query storage api wants project to be like this
parent = f"projects/{project_id}"

# Pass parent along with the empty arrow table to a read session
session = client.create_read_session(
    parent=parent,
    read_session=read_session,
    max_stream_count=1,
)

# Set up the stream to loop through all pages
stream = session.streams[0]
reader = client.read_rows(stream.name)

# Read all the pages
pages = reader.rows(session).pages

# Initialize an empty list to store record batches
record_batches = []

# Iterate through pages and collect record batches
for page in pages:
    record_batch = page.to_arrow()
    record_batches.append(record_batch)

# Concatenate all record batches into a single Table
table = pa.Table.from_batches(record_batches)

# Convert to a polars DF with zero copy
pl_df = pl.from_arrow(table)

print(pl_df)

# Stop the timer
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.2f} seconds")
