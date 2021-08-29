import boto3
import json
from fake_web_events import Simulation

from botocore.config import Config

my_config = Config(
    region_name = 'sa-east-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

client = boto3.client('firehose', config=my_config)

#client = boto3.client('firehose')
#kms = boto3.client('kms', region_name='sa-east-1')


def put_record(event):
    data = json.dumps(event) + "\n"
    response = client.put_record(
        DeliveryStreamName='kinesis-firehose-belisco',
        Record={"Data": data}
    )
    print(event)
    return response


simulation = Simulation(user_pool_size=100, sessions_per_day=10000)
events = simulation.run(duration_seconds=300)

for event in events:
    put_record(event)


