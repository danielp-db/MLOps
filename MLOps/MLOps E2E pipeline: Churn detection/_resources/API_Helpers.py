# Databricks notebook source
# Helper Functions
import mlflow
from mlflow.utils.rest_utils import http_request
import json

client = mlflow.tracking.client.MlflowClient()

host_creds = client._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

# COMMAND ----------

#Returns 
def get_job_starting_with(name_prefix):
    page_size = 25
    def get_all(page):
        r = http_request(host_creds=host_creds, endpoint="/api/2.1/jobs/list", method="GET", params={"limit": page_size, "offset": page*page_size}).json()
        if "jobs" in r:
          for j in r["jobs"]:
            if j["settings"]["name"].startswith(name_prefix):
              return j
          return get_all(page + 1)
        return None
    return get_all(0)
  
def get_churn_staging_job_id():
  job = get_job_starting_with("field_demos_churn_model_staging_validation")
  if job is not None:
    return job['job_id']
  else:
    #the job doesn't exist, we dynamically create it.
    #Note: requires DBR 10 ML to use automl model
    notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = '/'.join(notebook_path.split('/')[:-1])
    cloud_name = get_cloud_name()
    if cloud_name == "aws":
      node_type = "i3.xlarge"
    elif cloud_name == "azure":
      node_type = "Standard_DS3_v2"
    else:
      raise Exception(f"Cloud '{cloud_name}' isn't supported!")
    job_settings = {
                  "email_notifications": {},
                  "name": "field_demos_churn_model_staging_validation",
                  "max_concurrent_runs": 1,
                  "tasks": [
                      {
                          "new_cluster": {
                              "spark_version": "10.2.x-cpu-ml-scala2.12",
                              "spark_conf": {
                                  "spark.databricks.cluster.profile": "singleNode",
                                  "spark.master": "local[*, 4]"
                              },
                              "num_workers": 0,
                              "node_type_id": node_type,
                              "driver_node_type_id": node_type,
                              "custom_tags": {
                                  "ResourceClass": "SingleNode"
                              },
                              "spark_env_vars": {
                                  "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                              },
                              "enable_elastic_disk": True
                          },
                          "notebook_task": {
                              "notebook_path": f"{base_path}/05_job_staging_validation"
                          },
                          "email_notifications": {},
                          "task_key": "test-model"
                      }
                  ]
          }
    print("Job doesn't exists, creating it...")
    r = http_request(host_creds=host_creds, endpoint="/api/2.1/jobs/create", method="POST", json=job_settings).json()
    return r['job_id']

# COMMAND ----------

# Transition Request Events

# Trigger job
def create_job_webhook(model_name, job_id):
  trigger_job = json.dumps({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED"],
  "description": "Trigger the ops_validation job when a model is requested to move to staging.",
  "status": "ACTIVE",
  "job_spec": {
    "job_id": str(job_id),    # This is our 05_ops_validation notebook
    "workspace_url": host,
    "access_token": token
  }
  })
  response = mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_job)
  return(response)


# COMMAND ----------

# Notifications

#Webhooks can be used to send emails, Slack messages, and more.  In this case we #use Slack.  We also use `dbutils.secrets` to not expose any tokens, but the URL #looks more or less like this:

#`https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`

#You can read more about Slack webhooks [here](https://api.slack.com/messaging/webhooks#create_a_webhook).

# COMMAND ----------

import urllib 
import json 
import requests, json

def create_notification_webhook(model_name, slack_url):
  
  trigger_slack = json.dumps({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED"],
  "description": "Notify the MLOps team that a model is requested to move to staging.",
  "status": "ACTIVE",
  "http_url_spec": {
    "url": slack_url
  }
  })
  response = mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)
  return(response)

def send_notification(message):
  try:
    slack_webhook = dbutils.secrets.get("rk_webhooks", "slack")

    body = {'text': message}

    response = requests.post(
      slack_webhook, data=json.dumps(body),
      headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
      raise ValueError(
          'Request to slack returned an error %s, the response is:\n%s'
          % (response.status_code, response.text)
      )
  except:
    print("slack isn't properly setup in this workspace.")
    pass
  displayHTML(f"""<div style="border-radius: 10px; background-color: #adeaff; padding: 10px; width: 400px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 3px">
        <div style="padding-bottom: 5px"><img style="width:20px; margin-bottom: -3px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/bell.png"/> <strong>Churn Model update</strong></div>
        {message}
        </div>""")    

# COMMAND ----------

# Manage webhooks

# List
def list_webhooks(model_name):
  list_model_webhooks = json.dumps({"model_name": model_name})
  
  response = mlflow_call_endpoint("registry-webhooks/list", method = "GET", body = list_model_webhooks)
  return(response)

# Delete
def delete_webhooks(webhook_id):
  # Remove a webhook
  response = mlflow_call_endpoint("registry-webhooks/delete", method="DELETE",
                     body = json.dumps({'id': webhook_id}))
  return(response)

def reset_webhooks(model_name):
  whs = list_webhooks(model_name)
  if 'webhooks' in whs:
    for wh in whs['webhooks']:
      delete_webhooks(wh['id'])


# COMMAND ----------

# Request transition to staging
def request_transition(model_name, version, stage):
  
  staging_request = {'name': model_name,
                     'version': version,
                     'stage': stage,
                     'archive_existing_versions': 'true'}
  response = mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))
  return(response)
  
  
# Comment on model
def model_comment(model_name, version, comment):
  
  comment_body = {'name': model_name,
                  'version': version, 
                  'comment': comment}
  response = mlflow_call_endpoint('comments/create', 'POST', json.dumps(comment_body))
  return(response)

# COMMAND ----------

# After receiving payload from webhooks, use MLflow client to retrieve model details and lineage
def fetch_webhook_data(): 
  try:
    registry_event = json.loads(dbutils.widgets.get('event_message'))
    model_name = registry_event['model_name']
    model_version = registry_event['version']
    if 'to_stage' in registry_event and registry_event['to_stage'] != 'Staging':
      dbutils.notebook.exit()
  except:
    #If it's not in a job but interactive demo, we get the last version from the registry
    model_name = 'field_demos_customer_churn'
    model_version = client.get_registered_model(model_name).latest_versions[0].version
  return(model_name, model_version)

# COMMAND ----------

# Accept or reject transition request
def accept_transition(model_name, version, stage, comment):
  approve_request_body = {'name': model_details.name,
                          'version': model_details.version,
                          'stage': stage,
                          'archive_existing_versions': 'true',
                          'comment': comment}
  
  mlflow_call_endpoint('transition-requests/approve', 'POST', json.dumps(approve_request_body))

def reject_transition(model_name, version, stage, comment):
  
  reject_request_body = {'name': model_details.name, 
                         'version': model_details.version, 
                         'stage': stage, 
                         'comment': comment}
  
  mlflow_call_endpoint('transition-requests/reject', 'POST', json.dumps(reject_request_body))