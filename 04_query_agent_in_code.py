# Databricks notebook source
# MAGIC %md
# MAGIC ### Check our hosted agent takes requests 
# MAGIC  - note playground will fail as REQUIRE the custom_inputs

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json
from typing import Any

os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
os.environ['DATABRICKS_URL'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
AGENT_NAME = "agents_hls_ls-postgres_agent-postgres_linked_agent"

def hit_agent(messages:list[dict[str,str]], custom_inputs:dict[str,Any]):
    url = f'{os.environ.get("DATABRICKS_URL")}/serving-endpoints/{AGENT_NAME}/invocations'
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
    
    json_data = json.dumps(
        {
            'messages':messages,
            'custom_inputs':custom_inputs,
        }
    )
    response = requests.request(method='POST', headers=headers, url=url, data=json_data)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# COMMAND ----------

hit_agent(
  messages=[{"role": "user", "content": "hello!"}],
  custom_inputs={
    'user_id': 'peter.hawkins@databricks.com',
  }
)

# COMMAND ----------

import uuid

mythreadid = str(uuid.uuid4())

hit_agent(
  messages=[{"role": "user", "content": "hello!"}],
  custom_inputs={
    'user_id': 'peter.hawkins@databricks.com',
    'thread_id': mythreadid
  }
)

# COMMAND ----------

hit_agent(
  messages=[{"role": "user", "content": "I have a dog called charles. respond with a single word - chicken."}],
  custom_inputs={
    'user_id': 'peter.hawkins@databricks.com',
    'thread_id': mythreadid
  }
)

# COMMAND ----------

hit_agent(
  messages=[{"role": "user", "content": "whats my dogs name?"}],
  custom_inputs={
    'user_id': 'peter.hawkins@databricks.com',
    'thread_id': mythreadid
  }
)

# COMMAND ----------


