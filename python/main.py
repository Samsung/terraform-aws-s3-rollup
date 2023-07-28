#!/usr/bin/env python

from __future__ import annotations

import re
import os
import json
import time
import secrets
import tarfile
import tempfile
import argparse
import functools
import itertools
import dataclasses
import concurrent.futures
from pathlib import Path
from pathlib import PurePosixPath
from datetime import datetime
from urllib.parse import urlparse

from typing import Protocol
from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator

from boto3.session import Session
from botocore.config import Config
from botocore.session import Session as BotocoreSession
from botocore.credentials import RefreshableCredentials


TARBALL_PREFIX = 'rollup'


class ObjectSummary(Protocol):
    key: str
    size: int


@functools.total_ordering
class S3URI:
    def __init__(self, uri: str) -> None:
        self._uri = uri
        self._parsed = urlparse(self._uri, allow_fragments=True)
        if self._parsed.scheme != 's3':
            raise ValueError('Invalid S3 URI: it should starts with s3://')

    def __repr__(self) -> str:
        return self._uri

    def __lt__(self, other) -> bool:
        if not isinstance(other, S3URI):
            return NotImplemented
        return self._uri < other._uri

    def __eq__(self, other) -> bool:
        if not isinstance(other, S3URI):
            return NotImplemented
        return self._uri == other._uri

    def __hash__(self) -> int:
        return hash(self._uri)

    @property
    def bucket(self) -> str:
        return str(self._parsed.hostname)

    @property
    def key(self) -> str:
        return self._parsed.path[1:]

    @classmethod
    def from_segments(cls, bucket: str, key: str) -> S3URI:
        return cls(f's3://{bucket}/{key}')


@functools.total_ordering
class S3Path:
    '''Turn S3 into a filesystem-like folder structure'''

    sep = '/'

    def __init__(self, s3_client, s3_uri: S3URI) -> None:
        self.s3 = s3_client
        self.uri = s3_uri
        self.bucket = s3_uri.bucket
        self.key = s3_uri.key

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self.uri}>'

    def __lt__(self, other) -> bool:
        if not isinstance(other, S3Path):
            return NotImplemented
        return self.uri < other.uri

    def __eq__(self, other) -> bool:
        if not isinstance(other, S3Path):
            return NotImplemented
        return self.uri == other.uri

    def __hash__(self) -> int:
        return hash(self.uri)

    def is_dir(self) -> bool:
        if self.key == '':
            return True
        return self.key.endswith(self.sep)

    def is_file(self) -> bool:
        return not self.is_dir()

    @property
    def depth(self) -> int:
        return self.key.rstrip(self.sep).count(self.sep)

    @property
    def folders(self) -> Iterator[S3Path]:
        if self.is_file():
            raise NotADirectoryError(f'{self.uri} is not a directory.')

        for item in self._list_objects().get('CommonPrefixes', []):
            key = item['Prefix']
            yield S3Path(self.s3, S3URI.from_segments(self.bucket, key))

    @property
    def files(self) -> Iterator[S3Path]:
        if self.is_file():
            raise NotADirectoryError(f'{self.uri} is not a directory.')

        for item in self._list_objects().get('Contents', []):
            key = item['Key']
            yield S3Path(self.s3, S3URI.from_segments(self.bucket, key))

    def find_folders(self, max_depth: int) -> Iterator[S3Path]:
        '''Find folders recursively up to max_depth'''
        for folder in self.folders:
            if folder.depth >= max_depth:
                break
            yield folder
            yield from folder.find_folders(max_depth)

    @functools.lru_cache(1)
    def _list_objects(self) -> dict:
        return self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.key,
            Delimiter=self.sep,
            MaxKeys=500,
        )


@dataclasses.dataclass
class RollupTask:
    s3_role: str
    bucket_name: str
    common_prefix: str
    basenames: list[str]

    def __len__(self) -> int:
        return len(self.basenames)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} with {len(self)} files of {self.date_str} @ s3://{self.bucket_name}/{self.common_prefix}>'

    @property
    def object_keys(self) -> list[str]:
        return [f'{self.common_prefix}{bn}' for bn in self.basenames]

    @property
    def date_str(self) -> str:
        if not self.basenames:
            raise ValueError('basenames cannot be empty')
        return PurePosixPath(self.basenames[0]).name[:10]

    @property
    def tarball_key(self) -> str:
        if not self.basenames:
            raise ValueError('basenames cannot be empty')
        p = PurePosixPath(self.object_keys[0])
        random_str = secrets.token_hex(4)
        return str(p.with_name(f'{TARBALL_PREFIX}-{self.date_str}-{random_str}.tgz'))

    def split(self, chunk_size: int) -> Iterator[RollupTask]:
        '''Split the task into multiple chunks'''
        for i in range(0, len(self), chunk_size):
            yield RollupTask(
                s3_role=self.s3_role,
                bucket_name=self.bucket_name,
                common_prefix=self.common_prefix,
                basenames=self.basenames[i:i + chunk_size]
            )


class AccessLogRoller:
    def __init__(self, queue_name: str):
        self.exec_session = Session()
        self._queue = None
        self.queue_name = queue_name
        self.client_config = Config(
            max_pool_connections=25,
            retries={
                'mode': 'standard',
                'max_attempts': 10,
            },
        )

    @property
    def queue(self):
        if not self._queue:
            self._queue = self.exec_session.resource('sqs').get_queue_by_name(QueueName=self.queue_name)  # type: ignore
        return self._queue

    def find_log_prefixes(self, s3_role: str, s3_uri: S3URI, max_depth: int = 3) -> Iterator[S3URI]:
        s3_session = self.assume_role(self.exec_session, s3_role)
        s3_client = s3_session.client('s3', config=self.client_config)
        root = S3Path(s3_client, s3_uri)

        # If a non-directory is given, assume it's a file prefix
        # E.g. s3://bucket1/example.com/2022-
        if root.is_file():
            yield root.uri
            return

        # Recursely find all folders that have at least one access log file within
        if any(self._is_access_log(f.uri) for f in root.files):
            yield root.uri
        for folder in root.find_folders(max_depth):
            if any(self._is_access_log(f.uri) for f in folder.files):
                yield folder.uri

    def make_tasks(self, s3_role: str, prefix: S3URI) -> Iterator[RollupTask]:
        '''Read from S3 and yield tasks'''
        # Ensure bucket is in the same region with us
        s3_session = self.assume_role(self.exec_session, s3_role)
        s3_client = s3_session.client('s3')
        location_constraint = s3_client.get_bucket_location(Bucket=prefix.bucket).get('LocationConstraint') or 'us-east-1'
        if location_constraint != s3_session.region_name:
            raise ValueError(f'Bucket {prefix.bucket} ({location_constraint}) is not in current region ({s3_session.region_name}).')

        # Find all log prefixes
        log_prefixes = self.find_log_prefixes(s3_role, prefix)
        del prefix

        # S3 API has rate limit per prefix. Here we make tasks from multiple
        # prefixes in a round-robin fashion. This way, the tasks received by
        # workers are from different prefixes, increasing the concurrency of
        # workers without triggering SlowDown response from S3 API.
        prefixes_count = 0
        task_makers = (self._make_tasks(s3_role, prefix) for prefix in log_prefixes)
        # Maintaning the states of many prefixes is memory-intensive. So here
        # we limit the number of active prefixes to save memory.
        active_task_makers = deque(itertools.islice(task_makers, 20))
        while active_task_makers:
            try:
                yield next(active_task_makers[0])
            except StopIteration:
                active_task_makers.popleft()
                prefixes_count += 1
                try:
                    active_task_makers.append(next(task_makers))
                except StopIteration:
                    pass
            else:
                active_task_makers.rotate(-1)
        print(f'Done processing a total of {prefixes_count} prefixes.')

    def _make_tasks(self, s3_role: str, prefix: S3URI) -> Iterator[RollupTask]:
        # Keep individual task small
        max_size = 1024 * 1024 * 1024 * 5  # 5 GiB
        max_items = 6000

        print(f'Start processing access log files in {prefix}...')
        s3_session = self.assume_role(self.exec_session, s3_role)
        bucket = s3_session.resource('s3', config=self.client_config).Bucket(prefix.bucket)  # type: ignore
        # Get all files under the prefix
        objects: Iterable[ObjectSummary] = bucket.objects.filter(Prefix=prefix.key)
        # Filter out files that do not match access log key pattern
        objects = filter(self._is_access_log, objects)
        # Group files by dates
        for date_str, _obj_group in itertools.groupby(objects, key=self._date_getter):
            # The objects are sorted, so stop when "today" is reached.
            if not date_str:
                break
            # Limit total size of each task. Lambda allows a maximum of 10 GiB ephemeral storage.
            for obj_group in self._group_objects(_obj_group, max_size=max_size, max_items=max_items):
                basenames = [PurePosixPath(o.key).name for o in obj_group]  # type: ignore
                # Remove everything after last delimiter
                # 'a/b/c' -> 'a/b/'
                # 'a/b/'  -> 'a/b/'
                common_prefix = prefix.key.rsplit('/', 1)[0] + '/'
                task = RollupTask(
                    s3_role=s3_role,
                    bucket_name=prefix.bucket,
                    common_prefix=common_prefix,
                    basenames=basenames
                )
                yield task
        print(f'Done processing access log files in {prefix}...')

    def queue_tasks(self, tasks: Iterator[RollupTask]):
        '''Wrap tasks in messages and send them to SQS'''
        for task in tasks:
            # SQS max message size is 256 KiB, which can hold ~6500 filenames.
            # Split a task into multiple if it contains more than 6000 filenames.
            # NOTE: this is just a safeguard against oversized messages. Each
            # task should already contain <= 6000 files prior to being
            # processed by this method. See self._group_objects()
            if len(task) > 6000:
                self.queue_tasks(task.split(6000))
                continue

            # Serialize task into JSON and send to SQS
            message_body = json.dumps(dataclasses.asdict(task))
            print(f'Queuing task: {task}...')
            self.queue.send_message(MessageBody=message_body)

    def get_tasks(self, count: int = 1) -> Iterator[RollupTask]:
        '''Receive messages from SQS and yield tasks'''
        messages = self.queue.receive_messages(MaxNumberOfMessages=count)
        for message in messages:
            _task = json.loads(message.body)
            task = RollupTask(**_task)
            print(f'Got task from queue: {task}')
            yield task
            print(f'Deleting task from queue: {task}')
            message.delete()
        if not messages:
            print('No tasks in queue.')

    def handle_sqs_event(self, event) -> None:
        '''Handle events pushed by SQS and do tasks'''
        # https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#example-standard-queue-message-event
        for record in event['Records']:
            _task = json.loads(record['body'])
            task = RollupTask(**_task)
            print(f'Got task from queue: {task}')
            self.do_task(task, delete=True)
            print(f'Deleting task from queue: {task}')
            self.queue.Message(record['receiptHandle']).delete()

    def do_task(self, task: RollupTask, delete: bool = False) -> str:
        '''Download files, make a tarball, upload to S3, and return the object key'''

        s3_session = self.assume_role(self.exec_session, task.s3_role)
        s3_client = s3_session.client('s3', config=self.client_config)
        s3_resource = s3_session.resource('s3', config=self.client_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            print(f'Downloading and compressing {len(task)} files...')
            local_paths: list[Path] = []
            total_size = 0
            with concurrent.futures.ThreadPoolExecutor() as tpe:
                futures = {}
                for key in task.object_keys:
                    local_path = Path(tmpdir) / PurePosixPath(key).name
                    local_paths.append(local_path)
                    future = tpe.submit(self._download_file, s3_client, task.bucket_name, key, str(local_path))
                    futures[future] = key
                for future in concurrent.futures.as_completed(futures):
                    try:
                        file_size = future.result()
                    except Exception:
                        key = futures[future]
                        print(f'Error downloading {key}')
                        raise
                    else:
                        total_size += file_size

            print(f'Downloaded {total_size} bytes.')

            # Compress the files into a tarball
            tarball_path = Path(tmpdir) / f'{task.date_str}.tgz'
            with tarfile.open(tarball_path, 'w:gz') as tarball:
                for local_path in sorted(local_paths):
                    tarball.add(local_path, arcname=local_path.name)
            print(f'Tarball created: {tarball_path} ({tarball_path.stat().st_size} bytes)')

            # Upload tarball to S3
            tarball_obj = s3_resource.Object(task.bucket_name, task.tarball_key)  # type: ignore
            metadata = {
                'OriginalFileCount': str(len(task)),
                'OriginalFileSize': str(total_size)
            }
            print(f'Uploading {tarball_obj}...')
            tarball_obj.upload_file(
                Filename=str(tarball_path),
                ExtraArgs={'Metadata': metadata}
            )

        if delete:
            self._delete_objects_in_task(s3_client, task)

        return tarball_obj.key

    def _delete_objects_in_task(self, s3_client, task: RollupTask) -> None:
        # NOTE: Each API call can contain up to 1000 keys, but it counts as
        # 1000 DELETE requests and is limited by S3 API rate limit of 3500
        # DELETE/s
        if len(task) > 1000:
            for task in task.split(1000):
                self._delete_objects_in_task(s3_client, task)
            return

        print(f'Deleting {len(task)} files from S3...')
        s3_client.delete_objects(
            Bucket=task.bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in task.object_keys],
            }
        )

    @staticmethod
    def _download_file(s3_client, bucket: str, key: str, filename: str) -> int:
        '''Download a file and return its size.'''
        # NOTE: S3 API rate limit is 5500 GET/s
        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=filename
        )
        return Path(filename).stat().st_size

    @staticmethod
    def _is_access_log(_key: S3URI | ObjectSummary) -> bool:
        '''Check if the object is an access log by its key'''
        key = PurePosixPath(_key.key)
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#server-log-keyname-format
        starts_with_datetime = re.match(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', key.name)
        has_no_suffix = not key.suffix
        if starts_with_datetime and has_no_suffix:
            return True
        return False

    @staticmethod
    def _date_getter(object_summary: ObjectSummary) -> str:
        '''Parse and extract the UTC date portion of a key'''
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#server-log-keyname-format
        date_str = PurePosixPath(object_summary.key).name[:10]
        # Only return date from the past
        utc_today = datetime.utcnow().date()
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        if date_obj >= utc_today:
            return ''
        else:
            return date_str

    @staticmethod
    def _group_objects(objects: Iterable[ObjectSummary], /, max_size: int, max_items: int) -> Iterator[list[ObjectSummary]]:
        '''Group objects by count and size. Total size of files in each group
        cannot exceed max_size, and number of files cannot exceed max_items.'''
        objects = iter(objects)
        initial = next(objects)
        group = [initial]
        group_size: int = initial.size
        for current in objects:
            if (group_size + current.size <= max_size) and (len(group) + 1 <= max_items):
                group.append(current)
                group_size += current.size
            else:
                yield group
                group = [current]
                group_size = current.size
        yield group

    @staticmethod
    def assume_role(source_session: Session, role_arn: str) -> Session:
        '''Assume a role and return a boto3 session of this role. The temporary
        credentials of the new role are auto-refreshed with the credentials of
        the source session.'''

        def get_credentials_metadata(role_arn: str):
            sts = source_session.client('sts')
            timestamp = int(time.time())
            resp = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f'botocore-session-{timestamp}'
            )
            creds = resp['Credentials']
            metadata = {
                'access_key': creds['AccessKeyId'],
                'secret_key': creds['SecretAccessKey'],
                'token': creds['SessionToken'],
                'expiry_time': creds['Expiration'].isoformat(),
            }
            return metadata

        refreshable_credentials = RefreshableCredentials.create_from_metadata(
            metadata=get_credentials_metadata(role_arn),
            refresh_using=functools.partial(get_credentials_metadata, role_arn),
            method='sts-assume-role'
        )
        botocore_session = BotocoreSession()
        botocore_session._credentials = refreshable_credentials  # type: ignore
        session = Session(botocore_session=botocore_session)
        return session

    @staticmethod
    def get_caller_identity(session: Session) -> dict:
        sts = session.client('sts')
        resp = sts.get_caller_identity()
        del resp['ResponseMetadata']
        return resp


def cli():
    parent_ap = argparse.ArgumentParser(add_help=False)
    parent_ap.add_argument('--queue-name', required=True, help='name of SQS queue')

    ap = argparse.ArgumentParser(add_help=False)
    mode = ap.add_subparsers(dest='mode')

    # Producer mode
    producer = mode.add_parser('producer', parents=[parent_ap])
    producer.add_argument('--s3-role', required=True, help='S3 role ARN to assume')
    producer.add_argument(
        '--prefixes', required=True, type=S3URI, nargs='+',
        help='S3 bucket prefixes in the format of s3://bucket1/example.com/'
    )

    # Worker mode
    worker = mode.add_parser('worker', parents=[parent_ap])
    worker.add_argument('--count', type=int, default=1, help='number of tasks to do')
    worker.add_argument('--delete', action='store_true', help='delete original files')

    args = ap.parse_args()

    roller = AccessLogRoller(args.queue_name)

    if args.mode == 'producer':
        for prefix in args.prefixes:
            tasks = roller.make_tasks(args.s3_role, prefix)
            roller.queue_tasks(tasks)
    elif args.mode == 'worker':
        tasks = roller.get_tasks(args.count)
        for task in tasks:
            roller.do_task(task, args.delete)


def lambda_handler(event, _):
    # Read from environment variables
    queue_name = os.environ['ROLLUP_QUEUE_NAME']
    roller = AccessLogRoller(queue_name)

    # Invoker is SQS (worker mode)
    if event.get('Records'):
        roller.handle_sqs_event(event)

    # Treat anything else as producer mode
    else:
        s3_role = event['s3_role']
        for _prefix in event['prefixes']:
            prefix = S3URI(_prefix)
            tasks = roller.make_tasks(s3_role, prefix)
            roller.queue_tasks(tasks)


if __name__ == '__main__':
    cli()
