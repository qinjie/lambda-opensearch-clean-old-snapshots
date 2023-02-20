import boto3
from requests_aws4auth import AWS4Auth

from opensearch_utils import register_repository, list_all_repositories, list_snapshots_in_repo, get_snapshot_status, \
    take_snapshot, restore_latest_snapshot, delete_one_repository, delete_one_snapshot, get_snapshot, close_index, \
    get_latest_snapshot, list_indices, list_snapshots_in_progress, restore_one_snapshot

# # Settings
# host_sources = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]  # 源头域终端节点
# host_targets = [('<DOMAIN_ENDPOINT_WITH_HTTPS>','<REPOSITORY_NAME>','<S3_BUCKET_NAME>')]   # 目标域终端节点
# region = '<AWS_REGION>'  # S3桶的区域
# role_arn = '<ARN_OF_IAM_ROLE_LAMBDA>'  # Lambda函数的角色ARN

# Settings
host_sources = [
    ('https://vpc-mydomain-su6vi7ww5kwtqkojjfd5uw3xly.ap-southeast-1.es.amazonaws.com', 'my-repo',
     'elasticsearch-snapshots-460453255610')]  # mydomain
host_targets = [
    ('https://vpc-mydomain-2-wqolsf5ku4j5e3ubmpdkep4jxm.ap-southeast-1.es.amazonaws.com', 'my-repo',
     'elasticsearch-snapshots-460453255610')]  # mydomain-2
region = 'ap-southeast-1'
role_arn = 'arn:aws:iam::460453255610:role/ElasticSearchSnapshotLambdaRole'

# Get region and credential
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service,
                   session_token=credentials.token)


def lambda_handler(event, context):
    # Registration repo for source domains 源域
    #for host, repo, bucket in host_sources:
    #    register_a_repo(host, repo, bucket)

    # Register repo for target domains 目标域
    #for host, repo, bucket in host_targets:
    #    register_a_repo(host, repo, bucket)

    # # Take snapshot of source domains 源域
    #for host, repo, _ in host_sources:
    #    take_a_snapshot(host, repo)
    #
    # # Restore last snapshot to target domains
    # for host, repo, _ in host_targets:
    #     restore_latest_snapshot(host, awsauth, repo)

    # Restore a snapshot by name
    for host, repo, _ in host_targets:
        restore_one_snapshot(host, awsauth, repo, '20221207-030441')

    # # List indices in source domains
    # for host, _, _ in host_sources:
    #     list_indices(host, awsauth)
    # # List indices in target domain
    # for host, _, _ in host_sources:
    #     list_indices(host, awsauth)

    return {
        'statusCode': 200
    }


def register_a_repo(host: str, repo: str, bucket: str):
    # Register a repository
    register_repository(host, awsauth, repo, bucket, region, role_arn)

    # List all repositories
    list_all_repositories(host, awsauth)


def take_a_snapshot(host: str, repo: str):
    # Create a snapshot
    snapshot_name = take_snapshot(host, awsauth, repo)
    if snapshot_name is None:
        return None

    # Get snapshot in-progress
    get_snapshot_status(host, awsauth, repo_name=repo, snapshot_name=snapshot_name)

    return snapshot_name


