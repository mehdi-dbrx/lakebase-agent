# Databricks notebook source
# MAGIC %md
# MAGIC ### Make a UC function as a helper to make a connection string to Lakebase
# MAGIC  - I don't think this is really necessary as you could use a Workspace client, but it does address one issue:
# MAGIC    - there is a slight difference in how mlflow.predict validation check and model serving actually work - where WorkspaceClient for SP works fine on modelserving but not for pre validation check (due to system databricks token). Just using the Workspace Client and ignoring the pre validation is likely fine, but for now this works and can swap back later. 

# COMMAND ----------

# MAGIC %pip install databricks-langchain
# MAGIC %restart_python

# COMMAND ----------

from databricks_langchain import (
    DatabricksFunctionClient,
    set_uc_function_client,
)

client = DatabricksFunctionClient()
set_uc_function_client(client)

# COMMAND ----------

# MAGIC %md
# MAGIC ### You'll need to make a service principal
# MAGIC  - give the SP access to Lakebase via the permissions tab on the lakebase instance
# MAGIC  - also in the lakebase instance give the SP a postgreSQL role
# MAGIC  - create an OAUTH token for the service principal (from the SPs UI page)
# MAGIC  - place the OAUTH token in a databricks secret space
# MAGIC  - provide the SP id below, and the secret scope and key in which you placed the oauth token
# MAGIC
# MAGIC #### also provde catalog/schema for storing the UC function

# COMMAND ----------

INSTANCE_NAME = 'pgh-stateful-backend'
service_principal_id = '833d0305-84b4-4746-a67c-e84eec3b7712'
service_principal_oauth_token = dbutils.secrets.get("pgh", "lakebase_sp_oauth")

CATALOG = 'hls_ls'
SCHEMA = 'postgres_agent'

# COMMAND ----------

def lakebase_client(
    databricks_host: str, 
    service_principal_id: str,
    service_principal_oauth_token: str,
    service_principal_bearer_token: str,
    database_name: str,
    ) -> str:
    """ resturns a db_uri for use with Lakebase Database
    """
    import requests
    import json
    import uuid
    from typing import Optional, List

    class LakeBaseClient():
        def __init__(
          self, 
          databricks_host :str, 
          service_principal_id : str, 
          service_principal_bearer_token : str, 
          api_prefix : str= '/api/2.0/database'
        ):
          self.databricks_host = databricks_host
          self.service_principal_id = service_principal_id
          self.service_principal_bearer_token = service_principal_bearer_token
          self._workspace_client = requests.Session()
          self._workspace_client.headers.update({"Authorization": f"Bearer {self.service_principal_bearer_token}"})
          self._workspace_client.headers.update({"Content-Type": "application/json"})
          self.api_prefix = api_prefix

        def _make_url(self, path):
          return f"{self.databricks_host.rstrip('/')}/{path.lstrip('/')}"

        def get_read_write_dns(self, database_name) -> str:
          url = self._make_url(f"{self.api_prefix}/instances/{database_name}")
          response = self._workspace_client.get(url)
          return response.json().get('read_write_dns')
          
        def get_credential(
          self, 
          instance_names: Optional[List[str]] = None,
          request_id: Optional[str] = None,
          ) -> str:
          url = self._make_url(f"{self.api_prefix}/credentials")
          # print(f'getting credential with url : {url}')
          body = {}
          
          if instance_names is not None:
            body["instance_names"] = [v for v in instance_names]
          if request_id is not None:
            body["request_id"] = request_id
          headers = {
              "Accept": "application/json",
              "Content-Type": "application/json",
          }
          resp = self._workspace_client.post(
            url,
            headers=headers,
            json=body
          )
          # print('credential response: ')
          # print(resp.json())
          if 'token' not in resp.json():
            raise Exception(f"Error getting credential: {resp.json()}")
          return resp.json()['token']

        def get_db_uri(self, database_name):
          try:
            read_write_dns = self.get_read_write_dns(database_name)
          except Exception as e:
            print(f'error getting read_write_dns: {e}')
            raise e
          try:
            token = self.get_credential(instance_names=[database_name], request_id = str(uuid.uuid4()))
          except Exception as e:
            print(f'error getting credential: {e}')
            raise e
          return f"postgresql://{self.service_principal_id}:{token}@{read_write_dns}:5432/databricks_postgres?sslmode=require"

    def _get_sp_token(base_url, sp_id, sp_oauth_token):
      token_url = f"{base_url}/oidc/v1/token"
      response = requests.post(
          token_url,
          auth=(sp_id, sp_oauth_token),
          data={'grant_type': 'client_credentials', 'scope': 'all-apis'}
      )
      token = response.json().get('access_token')
      return token

    if service_principal_bearer_token == '':
      service_principal_bearer_token = _get_sp_token(
        base_url=databricks_host,
        sp_id=service_principal_id,
        sp_oauth_token=service_principal_oauth_token,
      )
      
    return LakeBaseClient(
      databricks_host=databricks_host,
      service_principal_id=service_principal_id,
      service_principal_bearer_token=service_principal_bearer_token,
    ).get_db_uri(database_name)

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# COMMAND ----------

lakebase_client(
    databricks_host=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
    service_principal_id = service_principal_id,
    service_principal_oauth_token = service_principal_oauth_token,
    service_principal_bearer_token='',
    database_name=INSTANCE_NAME,
)

# COMMAND ----------

client.create_python_function(
  func=lakebase_client,
  catalog=CATALOG,
  schema=SCHEMA,
  replace=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try it

# COMMAND ----------

myfn = client.get_function_as_callable(f'{CATALOG}.{SCHEMA}.lakebase_client')
myfn(
    databricks_host=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
    service_principal_id = service_principal_id,
    service_principal_oauth_token = service_principal_oauth_token,
    service_principal_bearer_token='',
    database_name=INSTANCE_NAME,
)

# COMMAND ----------

# MAGIC %md
# MAGIC check what errors look like...

# COMMAND ----------

myfn = client.get_function_as_callable(f'{CATALOG}.{SCHEMA}.lakebase_client')
myfn(
    databricks_host=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
    service_principal_id = service_principal_id,
    service_principal_oauth_token = service_principal_oauth_token,
    service_principal_bearer_token='',
    database_name=INSTANCE_NAME,
)

# COMMAND ----------


