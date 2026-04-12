"""
SQS es el servicio de cola de AWS. Lo usaremos a almacenar las acta, que luego
serán buscadas y procesadas tanto en la ingesta semanal como en la histórica

En este archivo se almacena la lógica de enviar batches de actas y contar mensajes 
visibles/vuelo para entender como avanza la ingesta
"""

import boto3
import json
import time
from typing import List, Dict, Tuple
from src.config import settings

def _get_sqs_client():
    return boto3.client(
        'sqs',
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION
    )

def contar_mensajes() -> Tuple[int, int]:
    """Retorna (mensajes_visibles, mensajes_en_vuelo)"""
    sqs = _get_sqs_client()
    resp = sqs.get_queue_attributes(
        QueueUrl=settings.SQS_QUEUE_URL,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    a = resp["Attributes"]
    return int(a["ApproximateNumberOfMessages"]), int(a["ApproximateNumberOfMessagesNotVisible"])

def enviar_batch_actas(lista_actas: List[Dict]):
    """
    Recibe una lista de diccionarios de actas y las envía a SQS en lotes de 10.
    Maneja reintentos automáticamente.
    """
    sqs = _get_sqs_client()
    
    for i in range(0, len(lista_actas), 10):
        batch = lista_actas[i:i + 10]
        entries = [
            {
                'Id': str(item["nro_acta"]),
                'MessageBody': json.dumps(item)
            } 
            for item in batch
        ]

        for intento in range(3):
            if not entries:
                break
            
            response = sqs.send_message_batch(
                QueueUrl=settings.SQS_QUEUE_URL, 
                Entries=entries
            )
            
            fallidos = response.get('Failed', [])
            if not fallidos:
                break
                
            print(f"   ⚠️ {len(fallidos)} mensajes fallaron en SQS. Reintentando ({intento + 1}/3)...")
            ids_fallidos = {f['Id'] for f in fallidos}
            entries = [e for e in entries if e['Id'] in ids_fallidos]
            time.sleep(1)