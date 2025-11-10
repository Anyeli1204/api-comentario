import os
import json
import uuid
import boto3

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def _parse_body(event):
    """
    Soporta API Gateway proxy (body string) y ejecución directa (dict).
    """
    body = event.get('body', event)
    if isinstance(body, str):
        return json.loads(body)
    if isinstance(body, dict):
        return body
    raise ValueError("Body no es JSON válido")

def lambda_handler(event, context):
    # 1) Leer datos
    data = _parse_body(event)
    tenant_id = data['tenant_id']
    texto = data['texto']

    # 2) Variables de entorno
    table_name = os.environ['TABLE_NAME']
    ingest_bucket = os.environ['INGEST_BUCKET']

    # 3) Construir comentario + UUID
    uid = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uid,
        'detalle': {'texto': texto}
    }

    # 4) Guardar en DynamoDB
    table = dynamodb.Table(table_name)
    ddb_resp = table.put_item(Item=comentario)

    # 5) Ingesta Push a S3
    s3_key = f"{tenant_id}/{uid}.json"
    s3.put_object(
        Bucket=ingest_bucket,
        Key=s3_key,
        Body=json.dumps(comentario, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json'
    )

    # 6) Respuesta
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "comentario": comentario,
            "s3": {"bucket": ingest_bucket, "key": s3_key}
        })
    }
