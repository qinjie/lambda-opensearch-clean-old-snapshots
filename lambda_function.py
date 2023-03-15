# lambda_function.py
import time
import boto3
import logging
from requests_aws4auth import AWS4Auth
from opensearch_utils import list_snapshots_in_repo, delete_one_snapshot, clean_repo, get_snapshot_status
from threading import Thread

# boto3.set_stream_logger('botocore', logging.DEBUG)

# # Settings
# host_sources = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]  # 源头域终端节点
# host_targets = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]   # 目标域终端节点
# region = '<AWS_REGION>'  # S3桶的区域
# role_arn = '<ARN_OF_IAM_ROLE_LAMBDA>'  # Lambda函数的角色ARN

# Settings
host_sources = [
    ('https://vpc-myelastic-elo6xfgh2ccj663y4xhpwra26y.ap-southeast-1.es.amazonaws.com', 'my-repo', 'elasticsearch-snapshots-460453255610')
    ]
region = 'ap-southeast-1'
role_arn = 'arn:aws:iam::460453255610:role/ElasticSearchSnapshotLambdaRole'

# Get region and credential
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service,
                   session_token=credentials.token)

# How many snapshots to keep
MIN_SNAPSHOT_COUNT = 1
BATCH_SIZE = 2
THREAD_TIMEOUT = 5 # seconds

def lambda_handler(event, context):

    # Get lists of old snapshot names for each repo
    snapshots_old_map = {}
    for host, repo, _ in host_sources:
        # List all snapshots in all repository
        print(f'List snapshots in {repo}')
        snapshots = list_snapshots_in_repo(host, repo, awsauth)

        snapshot_names = [i.get('snapshot') for i in snapshots]
        snapshots_old = snapshot_names[:-MIN_SNAPSHOT_COUNT]
        print('Old snapshots found:', snapshot_names[:BATCH_SIZE], '...')

        if snapshots_old:
            snapshots_old_map[f'{host},{repo}'] = snapshots_old

    # Delete one batch from each repo
    thread_map = {}
    for host_repo, snapshots_old in snapshots_old_map.items():
        host, repo = host_repo.split(',')
        batch = snapshots_old[:BATCH_SIZE]
        try:
            batch_names = ','.join(batch)
            print(f'Delete snapshots in {host}:', batch_names)

            # delete_one_snapshot(host, awsauth, repo, batch_names)
            thread = Thread(target=delete_one_snapshot, args=(host, awsauth, repo, batch_names))
            thread.start()
            thread_map[f'{host},{repo}'] = thread
        except Exception as ex:
            print('Exception:', ex)

    # Clean up files
    for host_repo, thread in thread_map.items():
        thread.join(timeout=THREAD_TIMEOUT)
        if not thread.is_alive():
            host, repo = host_repo.split(',')
            clean_repo(host, awsauth, repo)
        else:
            print(f'Snapshot deleting still in progress: {host_repo}')

    return {
        'statusCode': 200
    }
