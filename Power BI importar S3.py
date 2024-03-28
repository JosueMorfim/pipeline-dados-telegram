# Não esqueça de colocar suas informações corretamente

import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime

access_key_id = "SEU-ACESS-KEY-ID"

secret_access_key = "SEU-SECRET-ACESS-KEY"

bucket_name = "SEU-BUCKET-S3"

prefixo_arquivo = "telegram/"

particao_coluna = "context_date"

s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

objetos = s3.list_objects(Bucket=bucket_name, Prefix=f"{prefixo_arquivo}")

dfs = []

for obj in objetos.get('Contents', []):

    if obj['Key'].endswith('.parquet'):

        data_particao = obj['Key'].split('=')[1].split('/')[0]

        response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])

        df = pd.read_parquet(BytesIO(response['Body'].read()))

        df['data'] = pd.to_datetime(data_particao)

        dfs.append(df)

telegram= pd.concat(dfs, ignore_index=True)

print(telegram.head())