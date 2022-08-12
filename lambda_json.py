import json
import urllib.parse
from pprint import pprint
import boto3

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')

    # Retrieve payload bucket name
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Retrieve payload filename
    nomearquivo = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        
        # Fetch file from S3 bucket
        arquivo = s3.get_object(Bucket=bucket, Key=nomearquivo)
        
        # Deserialize file contents
        texto = arquivo['Body'].read().decode()
        dados = json.loads(texto)
        
        # Iteration to select columns and write data to DynamoDB
        for registros in dados:
            
            #Print of selected items
            #print(registros['Região'],registros['Sigla'],registros['Estado'],registros['População'])
            
            tabela = dynamodb.Table('populacao-pib-estados-brasil')
            tabela.put_item(Item={
                'regiao': registros['Região'],
                'uf': registros['Sigla'],
                'nome': registros['Estado'],
                'populacao': str(registros['População']),
                'pib': str(registros['PIB (R$)'])
            })

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(nomearquivo, bucket))
        raise e