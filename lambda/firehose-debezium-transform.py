import json
import base64
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OPERATION_MAP = {'c': 'insert', 'u': 'update', 'd': 'delete', 'r': 'insert'}
DATABASE_NAME = 'aurora_cdc'


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        try:
            # MSK source uses kafkaRecordValue, Kinesis source uses data
            raw = record.get('kafkaRecordValue') or record.get('data', '')
            payload = base64.b64decode(raw).decode('utf-8')
            cdc_event = json.loads(payload)

            op = cdc_event.get('op', 'c')
            operation = OPERATION_MAP.get(op, 'insert')

            source = cdc_event.get('source', {})
            table_name = source.get('table', 'unknown')

            data = cdc_event.get('before', {}) if op == 'd' else cdc_event.get('after', {})

            if not data:
                output.append({
                    'recordId': record['recordId'],
                    'result': 'Dropped',
                    'kafkaRecordValue': raw
                })
                continue

            encoded = base64.b64encode(
                (json.dumps(data) + '\n').encode('utf-8')
            ).decode('utf-8')

            output.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'kafkaRecordValue': encoded,
                'metadata': {
                    'otfMetadata': {
                        'destinationDatabaseName': DATABASE_NAME,
                        'destinationTableName': table_name,
                        'operation': operation
                    }
                }
            })

        except Exception as e:
            logger.error(f"Error: {str(e)}")
            output.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'kafkaRecordValue': record.get('kafkaRecordValue', record.get('data', ''))
            })

    logger.info(f"Processed {len(output)} records")
    return {'records': output}
