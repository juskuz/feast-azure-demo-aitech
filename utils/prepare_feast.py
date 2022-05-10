import os

from feast import FeatureStore
from azureml.core import Workspace
   
CONFIG_PATH = "/mnt/c/config.json" # change path if needed

def create_fs() -> FeatureStore:
    ws = Workspace.from_config(CONFIG_PATH)
    kv = ws.get_default_keyvault()
    os.environ['REGISTRY_PATH']=kv.get_secret("FEAST-REGISTRY-PATH")
    os.environ['SQL_CONN']=kv.get_secret("FEAST-OFFLINE-STORE-CONN")
    os.environ['REDIS_CONN']=kv.get_secret("FEAST-ONLINE-STORE-CONN")
    return FeatureStore("./feature_repo")
