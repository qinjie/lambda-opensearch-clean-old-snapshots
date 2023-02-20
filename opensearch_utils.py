from typing import Dict, List, Union

import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime


def get_snapshot_status(host: str, awsauth: AWS4Auth, repo_name: str = None, snapshot_name: str = None):
    """
    Retrieves a detailed description of the current state for each shard participating in the snapshot.
    """
    if repo_name and snapshot_name:
        path = f'/_snapshot/{repo_name}/{snapshot_name}/_status'
    elif repo_name:
        path = f'/_snapshot/{repo_name}/_status'
    else:
        path = f'/_snapshot/_status'

    url = host + path
    r = requests.get(url, auth=awsauth)
    print(f"Taking/restoring snapshot in progress: {r.text}")


def list_snapshots_in_repo(host: str, repo_name: str, awsauth: AWS4Auth) -> Dict:
    """
    List all snapshots in a repository
    """
    path = f'/_snapshot/{repo_name}/_all'
    url = host + path

    r = requests.get(url, auth=awsauth)
    snapshots = r.json().get("snapshots", [])
    print(f'Snapshot count = {len(snapshots)}')
    print(r.text)

    return snapshots


def list_all_repositories(host: str, awsauth: AWS4Auth):
    """
    List all repositories
    """
    path = '/_snapshot/_all'
    url = host + path

    r = requests.get(url, auth=awsauth)
    print(f"List of repositories: {r.text}")


def register_repository(host: str, awsauth: AWS4Auth, repo_name: str, bucket_name: str, region: str, role_arn: str):
    """
    Register a snapshot repository
    """
    path = f'/_snapshot/{repo_name}'
    url = host + path

    payload = {
        "type": "s3",
        "settings": {
            "bucket": bucket_name,
            "region": region,
            "role_arn": role_arn
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.put(url, auth=awsauth, json=payload, headers=headers)
    print(f"Registering a repo: {repo_name}")
    print(r.text)


def take_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str = None) -> Union[str, None]:
    """
    Take a snapshot in a repo. If snapshot_name is omitted, it will use current datetime string as name.
    Return snapshot name.
    """
    # Exit if there is snapshot in progress
    snapshots_in_progress = list_snapshots_in_progress(host, repo=repo_name, awsauth=awsauth)
    if snapshots_in_progress:
        print(f'In-progress Snapshot: {snapshots_in_progress}')
        print('Avoid running another snapshot')
        return None

    if snapshot_name is None:
        # Use current datetime as snapshot name
        now = datetime.now()
        snapshot_name = now.strftime("%Y%m%d-%H%M%S")
    path = f'/_snapshot/{repo_name}/{snapshot_name}'
    url = host + path

    r = requests.put(url, auth=awsauth)
    print(f"Taking a snapshot from repo {repo_name}: {snapshot_name}")
    print(r.text)

    return snapshot_name


def delete_one_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str):
    """
    Deletes a snapshot.
    """
    path = f'/_snapshot/{repo_name}/{snapshot_name}'
    url = host + path

    r = requests.delete(url, auth=awsauth)
    print(f"Deleting snapshot: {snapshot_name}")
    print(r.text)


def delete_one_repository(host: str, awsauth: AWS4Auth, repo_name: str):
    """
    Deletes a snapshot.
    """
    path = f'/_snapshot/{repo_name}'
    url = host + path

    r = requests.delete(url, auth=awsauth)
    print(f"Deleting a repository: {repo_name}")
    print(r.text)


def restore_latest_snapshot(host: str, awsauth: AWS4Auth, repo_name: str) -> bool:
    """
    Restore the latest snapshot in a repo. Restore all indexes except system indexes.
    """
    snapshots_in_progress = list_snapshots_in_progress(host, repo=repo_name, awsauth=awsauth)
    if snapshots_in_progress:
        print(f'In-progress Snapshot: {snapshots_in_progress}')
        print('Avoid restoring snapshot')
        return False

    # Get the latest snapshot
    latest_snapshot = get_latest_snapshot(host, repo_name, awsauth)
    if not latest_snapshot:
        return False

    snapshot_name = latest_snapshot.get('snapshot')

    # Close indices appeared in snapshot
    close_indices_in_snapshot(host, awsauth, repo_name, snapshot_name)

    # Start restoring
    restore_snapshot(host, awsauth, repo_name, snapshot_name)
    return True


def restore_one_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str) -> bool:
    """
    Restore a snapshot in a repo. Restore all indexes except system indexes.
    """
    if not is_snapshot_successful(host, repo_name, snapshot_name, awsauth):
        print('ERROR: Either snapshot not found or this snapshot not in SUCCESS state.')
        return False

    # Close indices appeared in snapshot
    close_indices_in_snapshot(host, awsauth, repo_name, snapshot_name)

    # Start restoring
    restore_snapshot(host, awsauth, repo_name, snapshot_name)
    return True


def close_indices_in_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str):
    # Get indices of the snapshot
    snapshot = get_snapshot(host, awsauth, repo_name, snapshot_name)
    # Close all indices in the snapshot before restore
    for index in snapshot['indices']:
        try:
            close_index(host, awsauth, index)
        except Exception as ex:
            print('[INFO] Index {index} not found in target domain.')


def restore_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str):
    """
    Restore snapshot (all indexes except Dashboards and fine-grained access control)
    """
    path = f'/_snapshot/{repo_name}/{snapshot_name}/_restore'
    url = host + path

    payload = {
        "indices": "*,-.kibana*,-.opendistro_security",  # All indices except .kibana* and .opendistro_security
        # "indices": "*,-.opendistro_security",  # All indices except .opendistro_security
        "include_global_state": True,
        "ignore_unavailable": True
    }
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, auth=awsauth, json=payload, headers=headers)
    print(f"Restoring from snapshot: {snapshot_name}")
    print(r.text)


def get_snapshot(host: str, awsauth: AWS4Auth, repo_name: str, snapshot_name: str):
    """
    Return information of a snapshot.
    Sample returned value:
        {
            "snapshot" : "20221130-054427",
            "uuid" : "5tsX98juSyWdPMXgXWSUgA",
            "version_id" : 7100299,
            "version" : "7.10.2",
            "indices" : [ ".kibana_1", "kibana_sample_data_ecommerce" ],
            "data_streams" : [ ],
            "include_global_state" : true,
            "state" : "SUCCESS",
            "start_time" : "2022-11-30T05:44:27.184Z",
            "start_time_in_millis" : 1669787067184,
            "end_time" : "2022-11-30T05:44:28.385Z",
            "end_time_in_millis" : 1669787068385,
            "duration_in_millis" : 1201,
            "failures" : [ ],
            "shards" : {
              "total" : 2,
              "failed" : 0,
              "successful" : 2
            }
        }
    """
    path = f'/_snapshot/{repo_name}/{snapshot_name}'
    url = host + path
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers)
    snapshots = r.json().get('snapshots', [])

    if len(snapshots) > 0:
        return snapshots[0]


def close_index(host: str, awsauth: AWS4Auth, index_name: str):
    """
    Close an index
    """
    path = f'/{index_name}/_close'
    url = host + path
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, auth=awsauth, json={}, headers=headers)
    print(f"Closing an index {index_name}: {r.text}")


def get_latest_snapshot(host: str, repo_name: str, awsauth: AWS4Auth) -> Dict:
    """
    Get information of the last successful snapshot.
    """
    # List all snapshots in all repository
    snapshots = list_snapshots_in_repo(host, repo_name, awsauth)
    # Sort snapshots by start_time
    sortedlist = sorted(snapshots, key=lambda d: d['start_time'])

    # Get the successful snapshot with the latest start_time
    for snapshot in sortedlist[::-1]:
        if snapshot.get("state") == "SUCCESS":
            print(f'Found latest snapshot in {repo_name}: {snapshot.get("snapshot")}')
            return snapshot

    print('No snapshot (state=SUCCESS) found.')


def list_indices(host: str, awsauth: AWS4Auth):
    """
    List all indices including docs count in the domain.
    """
    path = f'/_cat/indices?format=json'
    url = host + path
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers)
    print(f"Host: {host}, Indices: {r.text}")

    return r.json()


def list_snapshots_in_progress(host: str, repo: str, awsauth: AWS4Auth) -> List:
    """
    List all indices including docs count in the domain.
    """
    path = f'/_snapshot/{repo}/_current'
    url = host + path
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers)
    print(f"Host: {host}, snapshots in-progress: {r.text}")

    return r.json().get('snapshots', [])


def is_snapshot_successful(host: str, repo: str, snapshot_name: str, awsauth: AWS4Auth) -> bool:
    """
    Check if a snapshot is successful.
    """
    path = f'/_snapshot/{repo}/{snapshot_name}/_status'
    url = host + path
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers)
    print(f"Host: {host}, snapshot: {snapshot_name}, status: {r.text}")

    response = r.json()
    snapshots = r.json().get('snapshots', [])
    if not snapshots:
        print(f'ERROR: Snapshot {snapshot_name} not found. {r.text}')
        return False

    snapshot = snapshots[0]
    return snapshot.get('state') == 'SUCCESS'


def delete_latest_snapshot(host: str, repo: str, awsauth: AWS4Auth):
    """
    Delete the latest snapshot from repo
    """
    latest_snapshot = get_latest_snapshot(host, repo, awsauth)
    if latest_snapshot:
        snapshot_name = latest_snapshot.get('snapshot')
        delete_one_snapshot(host, awsauth, repo, snapshot_name=snapshot_name)
