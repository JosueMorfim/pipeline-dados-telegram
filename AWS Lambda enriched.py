import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def lambda_handler(event: dict, context: dict) -> bool:

  '''
  Diariamente é executado para compactar as diversas mensagensm, no formato
  JSON, do dia anterior, armazenadas no bucket de dados cru, em um único
  arquivo no formato PARQUET, armazenando-o no bucket de dados enriquecidos
  '''

  # vars de ambiente

  RAW_BUCKET = os.environ['AWS_S3_BUCKET']
  ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']

  # vars lógicas

  tzinfo = timezone(offset=timedelta(hours=-3))
  date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d')
  timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')

  # código principal

  table = None
  client = boto3.client('s3')

  try:

      response = client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f'telegram/context_date={date}')

      for content in response['Contents']:

        key = content['Key']
        client.download_file(RAW_BUCKET, key, f"/tmp/{key.split('/')[-1]}")

        with open(f"/tmp/{key.split('/')[-1]}", mode='r', encoding='utf8') as fp:

          data = json.load(fp)
          data = data["message"]

        parsed_data = parse_data(data=data)
        iter_table = pa.Table.from_pydict(mapping=parsed_data)

        if table:

          table = pa.concat_tables([table, iter_table])

        else:

          table = iter_table
          iter_table = None

      pq.write_table(table=table, where=f'/tmp/{timestamp}.parquet')
      client.upload_file(f"/tmp/{timestamp}.parquet", ENRICHED_BUCKET, f"telegram/context_date={date}/{timestamp}.parquet")

      return True

  except Exception as exc:
      logging.error(msg=exc)
      return False
      
      
      
def parse_data(data: dict) -> dict:

    date = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    parsed_data = dict()

    for key, value in data.items():

        if key == 'from':
            for k, v in data[key].items():
                if k in ['id', 'is_bot', 'first_name']:
                    parsed_data[f"{key if key == 'chat' else 'user'}_{k}"] = [v]

        elif key == 'chat':
            for k, v in data[key].items():
                if k in ['id', 'type']:
                    parsed_data[f"{key if key == 'chat' else 'user'}_{k}"] = [v]

        elif key in ['message_id', 'date', 'text', 'type']:
            parsed_data[key] = [value]
            parsed_data['type'] = ['text message']       
        
        elif key == 'video_note':
                parsed_data['text'] = ["new recording video"]
                parsed_data['type'] = ['video_note']          
        
        elif key == 'voice':
            if 'mime_type' in value:
                parsed_data['text'] = [f"voice message"]
                parsed_data['type'] = [f"{value['mime_type']} "]    
                
        elif key == 'document':
            if 'mime_type' in value:
                parsed_data['text'] = [f"{value['file_name']} "]
                parsed_data['type'] = [f"{value['mime_type']} "]    
                
        elif key == 'sticker':
            if 'emoji' in value:
                parsed_data['text'] = [f"{value['set_name']} - {value['emoji']}"]
                parsed_data['type'] = ['sticker']                   
             
        elif key == 'poll':
            if 'question' in value:
                parsed_data['text'] = [f"{value['question']} - options : {value['options']}"]
                parsed_data['type'] = ['poll']   
            
        elif key == 'photo':
            if 'caption' in data:
              parsed_data['text'] = [f"{data['caption']}"]
              parsed_data['type'] = ["photo"]
            else:
              parsed_data['text'] = ["image file"]
              parsed_data['type'] = ["photo"]

        elif key == 'location':
            if 'latitude' in value:
                parsed_data['text'] = [f"latitude: {value['latitude']} - longitude: {value['longitude']}"]
                parsed_data['type'] = ['location']  
      
        elif key == 'new_chat_participant':
            if 'first_name' in value:
                parsed_data['text'] = [f"{value['first_name']} joined on group"]
                parsed_data['type'] = ["new member"]      
       
              
        
    if 'text' not in parsed_data:
        parsed_data['text'] = ['NA']

    if 'type' not in parsed_data:
        parsed_data['type'] = ['NA']
        
        

    return parsed_data