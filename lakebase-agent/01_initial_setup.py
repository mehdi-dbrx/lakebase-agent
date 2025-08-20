# Databricks notebook source
# MAGIC %md
# MAGIC # Make Lakebase DB and Langraph tables for checkpointing

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC %pip install langgraph==0.3.4 langgraph-checkpoint-postgres
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

INSTANCE_NAME = "pgh-stateful-backend"

# Initialize the Workspace client
w = WorkspaceClient()

# get current username
username = spark.sql("SELECT current_user()").collect()[0][0]


# COMMAND ----------

username

# COMMAND ----------

# Create a database instance (only run once)

instance = w.database.create_database_instance(
   DatabaseInstance(
       name=INSTANCE_NAME,
       capacity="CU_1"
   )
)
print(f"Created database instance: {instance.name}")

# COMMAND ----------

import uuid
instance_name = INSTANCE_NAME
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])

# COMMAND ----------

instance = w.database.get_database_instance(name=instance_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note the compute may take a little while to spin up...
# MAGIC  - once ready we can check it (print its version)

# COMMAND ----------

import psycopg2

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
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)
conn.close()

# COMMAND ----------

instance.read_write_dns

# COMMAND ----------

DB_URI = f"postgresql://{username.replace('@','%40')}:{cred.token}@{instance.read_write_dns}:5432/databricks_postgres?sslmode=require"

# COMMAND ----------

# MAGIC %md
# MAGIC ### This bit actually make the tables for langgraph

# COMMAND ----------

from langgraph.checkpoint.postgres import PostgresSaver
checkpointer = PostgresSaver.from_conn_string(DB_URI)
with PostgresSaver.from_conn_string(DB_URI) as checkpointer:
    checkpointer.setup()

# COMMAND ----------

# MAGIC %md
# MAGIC Check the tables are there

# COMMAND ----------

import psycopg

with psycopg.connect(DB_URI) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        for table in tables:
            print(table[0])

# COMMAND ----------

with psycopg.connect(DB_URI) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, ("checkpoints",))
        columns = cur.fetchall()
        for col in columns:
            print(col)

# COMMAND ----------


