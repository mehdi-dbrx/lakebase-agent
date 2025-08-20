# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk
# MAGIC %pip install msgpack
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

# Initialize the Workspace client
w = WorkspaceClient()

import uuid
instance_name = "pgh-stateful-backend"
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
instance = w.database.get_database_instance(name=instance_name)

username = spark.sql("SELECT current_user()").collect()[0][0]

# COMMAND ----------

import psycopg2
import pandas as pd

# Connection parameters
conn = psycopg2.connect(
    host = instance.read_write_dns,
    dbname = "databricks_postgres",
    user = username,
    password = cred.token,
    sslmode = "require"
)

# Execute query
with conn.cursor() as cur:
    cur.execute("SELECT * FROM checkpoints")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    display(df)

conn.close()

# COMMAND ----------

import psycopg2
import pandas as pd

import msgpack

# Connection parameters
conn = psycopg2.connect(
    host = instance.read_write_dns,
    dbname = "databricks_postgres",
    user = username,
    password = cred.token,
    sslmode = "require"
)

# Execute query
with conn.cursor() as cur:
    cur.execute("SELECT * FROM checkpoint_blobs")
    rows = cur.fetchall()
    df_blobs = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    # blob column is binary string - convert back to regular
    df_blobs['blob_as_str'] = df_blobs['blob'].apply(lambda x: str(msgpack.unpackb(x.tobytes(), raw=False)))
    display(df_blobs)

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Further notes
# MAGIC
# MAGIC  - Should index over user_ids for faster lookups (eg for in app):
# MAGIC
# MAGIC  ```
# MAGIC  CREATE INDEX idx_checkpoints_metadata_userid
# MAGIC ON checkpoints((metadata->>'user_id'));
# MAGIC
# MAGIC -- now can do the following and it will auto use the index
# MAGIC SELECT DISTINCT thread_id FROM checkpoints WHERE metadata->>'user_id' = 'user_123';
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------


