from botocore.client import Config
from fastapi import FastAPI

import tempfile
import pickle
import boto3
import os

DEBUG            = os.getenv('DEBUG', False)
MINIO_URL        = os.getenv('MINIO_URL', 'http://127.0.0.1:9000')
MINIO_BUCKET     = os.getenv('MINIO_BUCKET', 'ranking')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'AKIAIOSFODNN7EXAMPLE')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

s3_client = None

def init_s3_client():
  global s3_client



def get_data():
  global MINIO_BUCKET, s3_client, ranking
  if not s3_client:
    try:
      if DEBUG:
        print("connecting: "+MINIO_URL)
      s3_client = boto3.client(
                    service_name='s3',
                    endpoint_url=MINIO_URL,
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                  )
    except:
      if DEBUG:
        print("ERROR: unable to connect to bucket")
      raise Exception('unable to connect to bucket')

  return pickle.loads(s3_client.get_object(Bucket=MINIO_BUCKET, Key='ranking.list')['Body'].read())

def set_data(ranking):
  global MINIO_BUCKET, s3_client
  if not s3_client:
    try:
      if DEBUG:
        print("connecting: "+MINIO_URL)
      s3_client = boto3.client(
                    service_name='s3',
                    endpoint_url=MINIO_URL,
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                  )
    except:
      if DEBUG:
        print("ERROR: unable to connect to bucket")
      raise Exception('unable to connect to bucket')

  tmp_ranking = tempfile.TemporaryFile()

  pickle.dump(ranking, tmp_ranking)
  tmp_ranking.seek(os.SEEK_SET)

  return s3_client.put_object(
                  Body=tmp_ranking,
                  Bucket=MINIO_BUCKET,
                  Key='ranking.list'
                )  

app = FastAPI()

@app.get("/health")
async def root():
    return { "status": "ready"}

@app.get("/auth_endpoint")
async def root():
    return {"auth": os.environ.get("AUTH_TOKEN", "AUTH")}

@app.get("/ranking")
async def get_ranking():
  try:
    return get_data()
  except:
    return []

@app.get("/ranking/{player}/{score}")
async def set_ranking(player, score):
  try:
    ranking = get_data()
  except:
    ranking = []

  new_ranking = []

  try:
    record = {'player': player, 'score': int(score)}
  except:
    record = {'player': player, 'score': 0}

  inserted = False

  if ranking:
    for item in ranking:
      if not inserted and item['score'] < record['score']:
        new_ranking.append(record)
        new_ranking.append(item)
        inserted = True
      elif not inserted and item['score'] == record['score'] and item['player'] == player:
        new_ranking.append(item)
        inserted = True
      elif not inserted and item['score'] == record['score']:
        new_ranking.append(item)
        new_ranking.append(record)
        inserted = True
      else:
        new_ranking.append(item)
    if not inserted:
      new_ranking.append(record)
  else:
    new_ranking.append(record)

  set_data(new_ranking[:10])

  return new_ranking[:10]