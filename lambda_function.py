import boto3
from requests_aws4auth import AWS4Auth

from opensearch_utils import register_repository, list_all_repositories, list_snapshots_in_repo, get_snapshot_status, \
    take_snapshot, restore_latest_snapshot, delete_one_repository, delete_one_snapshot, get_snapshot, close_index, \
    get_latest_snapshot, list_indices, list_snapshots_in_progress, restore_one_snapshot, delete_one_snapshot

# # Settings
# host_sources = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]  # 源头域终端节点
# host_targets = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]   # 目标域终端节点
# region = '<AWS_REGION>'  # S3桶的区域
# role_arn = '<ARN_OF_IAM_ROLE_LAMBDA>'  # Lambda函数的角色ARN

# Settings
host_sources = [
    ('https://vpc-myelastic-elo6xfgh2ccj663y4xhpwra26y.ap-southeast-1.es.amazonaws.com', 'my-repo',
     'opensearch-snapshots-460453255610')]  # myelastic
host_targets = [
    ('https://vpc-myopen-jxwadi4e5lexp4docguskpjrbi.ap-southeast-1.es.amazonaws.com', 'my-repo',
     'opensearch-snapshots-460453255610')]  # myopen
region = 'ap-southeast-1'
role_arn = 'arn:aws:iam::460453255610:role/ElasticSearchSnapshotLambdaRole'

# Get region and credential
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service,
                   session_token=credentials.token)

# How many snapshots to keep
MIN_SNAPSHOT_COUNT = 10


def lambda_handler(event, context):
    for host, repo, _ in host_sources:
        # List all snapshots in all repository
        snapshots = list_snapshots_in_repo(host, repo, awsauth)
        print(f'Found snapshots in {repo}: {snapshots}')

        # Get the successful snapshot with the latest start_time
        for snapshot in snapshots[:-MIN_SNAPSHOT_COUNT]:
            print(f'Checking snapshot: {snapshot.get("snapshot")}')
            delete_one_snapshot(host, awsauth, repo, snapshot.get("snapshot"))

    return {
        'statusCode': 200
    }


def init_repo():
    # List all repositories
    print('Source repo:')
    for host, repo, bucket in host_sources:
        list_all_repositories(host, awsauth)
    # Registeration repo for source domains 源域
    for host, repo, bucket in host_sources:
        register_a_repo(host, repo, bucket)

    # List all repositories
    print('Tar repo:')
    for host, repo, bucket in host_targets:
        list_all_repositories(host, awsauth)
    # Register repo for target domains 目标域
    for host, repo, bucket in host_targets:
        register_a_repo(host, repo, bucket)


def register_a_repo(host: str, repo: str, bucket: str):
    # Register a repository
    register_repository(host, awsauth, repo, bucket, region, role_arn)

    # List all repositories
    list_all_repositories(host, awsauth)


def delete_a_repo(host: str, repo: str):
    # Delete a repository
    delete_one_repository(host, awsauth, repo)


def take_a_snapshot(host: str, repo: str):
    # Create a snapshot
    snapshot_name = take_snapshot(host, awsauth, repo)
    if snapshot_name is None:
        return None

    # Get snapshot in-progress
    get_snapshot_status(host, awsauth, repo_name=repo,
                        snapshot_name=snapshot_name)

    return snapshot_name


def delete_latest_snapshot(host: str, repo: str):
    latest_snapshot = get_latest_snapshot(host, repo, awsauth)
    if latest_snapshot:
        snapshot_name = latest_snapshot.get('snapshot')
        delete_one_snapshot(host, awsauth, repo, snapshot_name=snapshot_name)
