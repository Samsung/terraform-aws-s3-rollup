import os
import tarfile
import tempfile
import dataclasses
from datetime import datetime
from pathlib import PurePosixPath

import boto3
import pytest
from moto import mock_s3
from moto import mock_sqs
from moto import mock_sts

import main
from main import S3URI
from main import S3Path
from main import RollupTask
from main import AccessLogRoller


class TestS3URI:
    def test_parse(self):
        uri = S3URI('s3://bucket1/example.com/foo/bar/')
        assert uri.bucket == 'bucket1'
        assert uri.key == 'example.com/foo/bar/'

    def test_parse_bucket_root(self):
        uri = S3URI('s3://bucket2/')
        assert uri.bucket == 'bucket2'
        assert uri.key == ''

    def test_invalid_uri(self):
        with pytest.raises(ValueError):
            S3URI('example.com/foo/bar/')


@mock_s3
class TestS3Path:

    bucket = 'bucket1'
    keys = [
        'root.jpg',
        'media/photo-1.jpg',
        'media/photo-2.jpg',
        'media/photo-3.jpg',
        'log/example.com/robots.txt',
        'log/example.com/2022-07-01-05-15-43-42117B9A70B0F1BA',
        'log/example.com/2022-07-01-05-21-46-66A49E4B82CEA312',
        'log/example.com/2022-07-01-05-31-51-2F572D0BD05E48EF',
        'log2/example.org/2022-07-01-05-15-43-42117B9A70B0F1BA',
        'log2/example.org/2022-07-01-05-21-46-66A49E4B82CEA312',
        'log2/example.org/2022-07-01-05-31-51-2F572D0BD05E48EF',
        'one/two/three/four/five/six/2022-07-02-00-21-50-EA56CA33F44CFE0E',
        'one/two/three/four/five/six/2022-07-02-00-41-28-C88B4724EC22EBB0',
        'one/two/three/four/five/six/2022-07-02-05-13-29-E4E5C6CF6881DCA8',
    ]

    def setup_s3(self):
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=self.bucket)
        for key in self.keys:
            s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=f'{key} body'
            )
        return s3

    def test_get_children(self):
        s3 = self.setup_s3()
        folder = S3Path(s3, S3URI.from_segments(self.bucket, ''))
        assert folder.uri == S3URI('s3://bucket1/')
        assert sorted(f.key for f in folder.files) == ['root.jpg']
        assert sorted(f.key for f in folder.folders) == sorted(['media/', 'log/', 'log2/', 'one/'])
        del folder

    def test_chdir(self):
        s3 = self.setup_s3()
        folder = S3Path(s3, S3URI.from_segments(self.bucket, 'log/'))
        assert folder.uri == S3URI('s3://bucket1/log/')
        assert len(list(folder.files)) == 0
        sub_folders = list(folder.folders)
        assert len(sub_folders) == 1
        assert sub_folders[0].key == 'log/example.com/'
        assert sub_folders[0].uri == S3URI('s3://bucket1/log/example.com/')

    def test_find_folders(self):
        s3 = self.setup_s3()
        root = S3Path(s3, S3URI.from_segments(self.bucket, ''))
        folders = root.find_folders(3)
        assert sorted(f.key for f in folders) == sorted([
            'media/',
            'log/',
            'log/example.com/',
            'log2/',
            'log2/example.org/',
            'one/',
            'one/two/',
            'one/two/three/',
        ])


class TestRollupTask:
    s3_role = 'arn:aws:iam::123456789012:role/s3-rollup-bucket-access'
    task = RollupTask(
        s3_role=s3_role,
        bucket_name='bucket1',
        common_prefix='example.com/',
        basenames=[
            '2022-06-30-16-13-48-F7334FE15D46A819',
            '2022-06-30-17-13-48-F7334FE15D46A819',
            '2022-06-30-18-13-48-F7334FE15D46A819',
            '2022-06-30-19-13-48-F7334FE15D46A819',
            '2022-06-30-20-13-48-F7334FE15D46A819',
            '2022-06-30-21-13-48-F7334FE15D46A819',
            '2022-06-30-22-13-48-F7334FE15D46A819',
            '2022-06-30-23-13-48-F7334FE15D46A819',
        ]
    )

    def test_date_str(self):
        assert self.task.date_str == '2022-06-30'

    def test_tarball_key(self):
        assert self.task.tarball_key.startswith('example.com/rollup-2022-06-30')
        assert self.task.tarball_key.endswith('.tgz')

    def test_empty_list(self):
        with pytest.raises(ValueError):
            task = RollupTask(self.s3_role, 'bucket2', common_prefix='', basenames=[])
            task.tarball_key

    def test_split(self):
        assert len(list(self.task.split(2))) == 4
        assert len(list(self.task.split(4))) == 2
        assert len(list(self.task.split(99))) == 1


@dataclasses.dataclass
class ObjectSummary:
    size: int


class TestUtils:

    def make_objects(self, *sizes):
        return [ObjectSummary(size=s) for s in sizes]

    def test_group_objects_by_total_size(self):
        obj1 = ObjectSummary(5)
        obj2 = ObjectSummary(8)
        obj3 = ObjectSummary(3)
        obj4 = ObjectSummary(3)
        obj5 = ObjectSummary(4)
        objects = [obj1, obj2, obj3, obj4, obj5]
        assert list(AccessLogRoller._group_objects(objects, max_size=10, max_items=999)) == [[obj1], [obj2], [obj3, obj4, obj5]]


@mock_sqs
@mock_s3
@mock_sts
class TestAccessLogRoller:
    prefix = S3URI('s3://bucket1/example.com/')
    s3_role = 'arn:aws:iam::123456789012:role/s3-rollup-bucket-access'
    queue_name = 's3-rollup-oregon'

    date_strs = [
        '2023-01-01',
        '2023-01-02',
        '2023-01-03',
        str(datetime.utcnow().date())
    ]
    time_strs = [
        '19-13-48-F7334FE15D46A819',
        '20-13-48-F7334FE15D46A819',
        '21-13-48-F7334FE15D46A819',
    ]
    sample_task = RollupTask(
        s3_role=s3_role,
        bucket_name=prefix.bucket,
        common_prefix=prefix.key,
        basenames=[
            f'{date_strs[0]}-{time_strs[0]}',
            f'{date_strs[0]}-{time_strs[1]}',
            f'{date_strs[0]}-{time_strs[2]}',
        ]
    )

    def setup_s3(self):
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=self.prefix.bucket)
        # Upload some valid files
        for date_str in self.date_strs:
            for time_str in self.time_strs:
                s3.put_object(
                    Bucket=self.prefix.bucket,
                    Key=f'{self.prefix.key}{date_str}-{time_str}',
                    Body='foo',
                )
        # Upload some invalid files
        for key in (
            'example.com/hello.txt',
            'webroot/index.html',
        ):
            s3.put_object(
                Bucket=self.prefix.bucket,
                Key=key,
                Body='{key} body'
            )

    def setup_sqs(self):
        sqs = boto3.client('sqs')
        sqs.create_queue(QueueName=self.queue_name)

    def test_make_tasks(self):
        self.setup_s3()
        roller = AccessLogRoller(self.queue_name)
        tasks = list(roller.make_tasks(self.s3_role, self.prefix))
        assert len(tasks) == 3
        assert self.sample_task in tasks

    def test_queue_and_get_tasks(self):
        self.setup_s3()
        self.setup_sqs()
        roller = AccessLogRoller(self.queue_name)
        tasks = list(roller.make_tasks(self.s3_role, self.prefix))
        roller.queue_tasks(tasks)
        received_tasks = list(roller.get_tasks(10))
        assert len(tasks) == len(received_tasks) == 3
        for rt in received_tasks:
            assert rt in tasks

        # Check if tasks are deleted from queue
        assert len(list(roller.get_tasks(10))) == 0

    def test_do_task_file_roller(self):
        self.setup_s3()
        roller = AccessLogRoller(self.queue_name)
        tarball_key = roller.do_task(self.sample_task)
        s3 = boto3.resource('s3')
        tarball_obj = s3.Object(self.prefix.bucket, tarball_key)  # type: ignore
        with tempfile.NamedTemporaryFile() as tmp:
            tarball_obj.download_fileobj(tmp)
            tmp.seek(0)
            with tarfile.open(tmp.name, 'r:gz') as tarball_local:
                filenames = tarball_local.getnames()
        orig_filenames = [PurePosixPath(k).name for k in self.sample_task.object_keys]
        assert filenames == orig_filenames

        # Ensure original files are not deleted
        for obj_key in self.sample_task.object_keys:
            obj = s3.Object(self.prefix.bucket, obj_key)  # type: ignore
            obj.load()

    def test_do_task_with_delete(self):
        from botocore.exceptions import ClientError
        self.setup_s3()
        roller = AccessLogRoller(self.queue_name)
        roller.do_task(self.sample_task, True)
        s3 = boto3.resource('s3')
        for obj_key in self.sample_task.object_keys:
            obj = s3.Object(self.prefix.bucket, obj_key)  # type: ignore
            with pytest.raises(ClientError, match='404.*Not Found'):
                obj.load()

    def test_skip_already_rolled_up_files(self):
        self.setup_s3()
        self.setup_sqs()
        roller = AccessLogRoller(self.queue_name)
        # Make and queue tasks
        tasks = roller.make_tasks(self.s3_role, self.prefix)
        roller.queue_tasks(tasks)
        del tasks
        # Receive and do tasks
        received_tasks = roller.get_tasks(10)
        for task in received_tasks:
            roller.do_task(task, delete=True)
        del received_tasks
        # Make tasks again
        tasks = list(roller.make_tasks(self.s3_role, self.prefix))
        assert len(tasks) == 0

    def test_lambda_handler(self):
        self.setup_s3()
        self.setup_sqs()
        os.environ['ROLLUP_QUEUE_NAME'] = self.queue_name
        event = {
            's3_role': self.s3_role,
            'prefixes': [
                's3://bucket1/example.com/'
            ]
        }
        main.lambda_handler(event, None)

    def test_is_access_log(self):
        good_key = S3URI('s3://bucket1/example.com/2013-11-01-21-32-16-E568B2907131C0C0')
        bad_key = S3URI('s3://bucket1/example.com/sample.jpg')
        assert AccessLogRoller._is_access_log(good_key)
        assert not AccessLogRoller._is_access_log(bad_key)

    def test_find_log_prefixes(self):
        self.setup_s3()
        roller = AccessLogRoller(self.queue_name)
        bucket_root = S3URI.from_segments(self.prefix.bucket, '')
        assert sorted(roller.find_log_prefixes(self.s3_role, bucket_root)) == sorted([
            S3URI('s3://bucket1/example.com/'),
        ])
