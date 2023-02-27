import boto3
from requests_aws4auth import AWS4Auth

from opensearch_utils import list_snapshots_in_repo, delete_one_snapshot, clean_repo

# # Settings
# host_sources = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]  # 源头域终端节点
# region = '<AWS_REGION>'  # S3桶的区域
# role_arn = '<ARN_OF_IAM_ROLE_LAMBDA>'  # Lambda函数的角色ARN

# Settings
host_sources = [
    ('https://vpc-myelastic-elo6xfgh2ccj663y4xhpwra26y.ap-southeast-1.es.amazonaws.com', 'my-repo',
     'opensearch-snapshots-460453255610')]  # myelastic
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

def lambda_handler(event, context):
    for host, repo, _ in host_sources:
        # List all snapshots in all repository
        snapshots = list_snapshots_in_repo(host, repo, awsauth)
        print(f'Found snapshots in {repo}: {snapshots}')

        snapshot_names = [i.get('snapshot') for i in snapshots]
        print('Snapshots found:', snapshot_names)

        snapshots_old = snapshot_names[:-MIN_SNAPSHOT_COUNT]
        print('Snapshots to delete:', snapshots_old if snapshots_old else 'None')

        if snapshots_old:
            batches_to_delete = [snapshots_old[i:i+BATCH_SIZE] for i in range(0, len(snapshots_old), BATCH_SIZE)]
            for batch in batches_to_delete:
                print('Deleting', batch)
                try:
                    delete_one_snapshot(host, awsauth, repo, ','.join(batch))
                except Exception as ex:
                    pass
            clean_repo(host, awsauth, repo)

    return {
        'statusCode': 200
    }
