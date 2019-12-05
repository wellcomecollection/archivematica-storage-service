import os
import random
import shutil
import tempfile
from StringIO import StringIO
import uuid

import boto3
import json
import mock
import pytest
import subprocess
from django.test import TestCase
from moto import mock_s3

from locations import models


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.abspath(os.path.join(THIS_DIR, '..', 'fixtures'))


def create_storage_service_response(status):
    resp = {
        "id": str(uuid.uuid4()),
        "callback": {
            "status": {"id": status}
        },
        "status": {"id": status},
        "events": [
            {"type": "IngestEvent", "description": "Something happened"},
            {"type": "IngestEvent", "description": "Something else happened"},
        ],
        "bag": {
            "info": {"externalIdentifier": str(uuid.uuid4())}
        },
    }

    if status == "succeeded":
        version = random.randint(1, 100)
        resp["bag"]["info"]["version"] = "v%d" % version

    return resp


@mock_s3
class WellcomeTestBase(TestCase):

    fixtures = ["base.json", "wellcome.json"]

    def setUp(self):
        self.wellcome_object = models.WellcomeStorageService.objects.get(id=1)

        self._s3 = boto3.client("s3", region_name="us-east-1")
        self._s3.create_bucket(Bucket=self.wellcome_object.s3_bucket)

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @property
    def package_uuid(self):
        return "6465da4a-ea88-4300-ac56-9641125f1276"

    def get_package(self):
        package = models.Package.objects.get(uuid=self.package_uuid)
        # Simulate the callback
        def set_package_to_uploaded(*args):
            package.status = models.Package.UPLOADED
            package.save()
        package.refresh_from_db = mock.Mock(side_effect=set_package_to_uploaded)
        return package


class TestWellcomeMoveFromStorageService(WellcomeTestBase):
    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_uploads_bag_to_s3_bucket(self, mock_wellcome_client_class, mock_sleep):
        package = self.get_package()
        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        assert self._s3.get_object(Bucket='ingest-bucket', Key='born-digital/bag.zip')

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_calls_wellcome_ss_client(self, mock_wellcome_client_class, mock_sleep):
        package = self.get_package()
        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        mock_wellcome_client_class.assert_called_with(
            api_url=self.wellcome_object.api_root_url,
            token_url=self.wellcome_object.token_url,
            client_id=self.wellcome_object.app_client_id,
            client_secret=self.wellcome_object.app_client_secret,
        )

        mock_wellcome_client_class.return_value.create_s3_ingest.assert_called_with(
            space_id='born-digital',
            s3_key='born-digital/bag.zip',
            s3_bucket=self.wellcome_object.s3_bucket,
            callback_url='https://test.localhost/api/v2/file/6465da4a-ea88-4300-ac56-9641125f1276/wellcome_callback/?username=username&api_key=api_key',
            external_identifier=package.uuid,
            ingest_type='create',
        )

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_updates_bag_if_reingest(self, mock_wellcome_client_class, mock_sleep):
        package = self.get_package()
        package.misc_attributes['bag_id'] = package.uuid
        package.save()
        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        mock_wellcome_client_class.assert_called_with(
            api_url=self.wellcome_object.api_root_url,
            token_url=self.wellcome_object.token_url,
            client_id=self.wellcome_object.app_client_id,
            client_secret=self.wellcome_object.app_client_secret,
        )

        mock_wellcome_client_class.return_value.create_s3_ingest.assert_called_with(
            space_id='born-digital',
            s3_key='born-digital/bag.zip',
            s3_bucket=self.wellcome_object.s3_bucket,
            callback_url='https://test.localhost/api/v2/file/6465da4a-ea88-4300-ac56-9641125f1276/wellcome_callback/?username=username&api_key=api_key',
            external_identifier=package.uuid,
            ingest_type='create',
        )

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_waits_for_callback(self, mock_wellcome_client_class, mock_sleep):
        package = self.get_package()
        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        assert package.refresh_from_db.call_count == 1

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_tries_fetching_ingest_if_no_callback(self, mock_wellcome_client_class, mock_sleep):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        package.current_path = "locations/fixtures/bag-6465da4a-ea88-4300-ac56-9641125f1276.zip"
        package.status = models.Package.STAGING
        package.save()

        storage_service_response = create_storage_service_response(status="succeeded")
        storage_service_response["bag"]["info"]["externalIdentifier"] = "external-id"
        storage_service_response["bag"]["info"]["version"] = "v3"

        mock_wellcome = mock_wellcome_client_class.return_value
        mock_wellcome.get_ingest_from_location.return_value = storage_service_response

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        package.refresh_from_db()
        assert package.status == models.Package.UPLOADED
        assert package.current_path == 'bag-6465da4a-ea88-4300-ac56-9641125f1276.zip'
        assert package.misc_attributes['wellcome.identifier'] == "6465da4a-ea88-4300-ac56-9641125f1276"
        assert package.misc_attributes['wellcome.version'] == 'v3'

    @mock.patch("time.sleep")
    @mock.patch("locations.models.wellcome.StorageServiceClient")
    def test_tries_fetching_failed_ingest_if_no_callback(
        self, mock_wellcome_client_class, mock_sleep
    ):
        package = models.Package.objects.get(
            uuid="6465da4a-ea88-4300-ac56-9641125f1276"
        )
        path = "locations/fixtures/bag-6465da4a-ea88-4300-ac56-9641125f1276.zip"
        package.current_path = path
        package.status = models.Package.STAGING
        package.save()

        storage_service_response = create_storage_service_response(status="failed")
        mock_wellcome = mock_wellcome_client_class.return_value
        mock_wellcome.get_ingest_from_location.return_value = storage_service_response

        with pytest.raises(models.StorageException, match="Failed to store package"):
            self.wellcome_object.move_from_storage_service(
                src_path=os.path.join(FIXTURES_DIR, "small_compressed_bag.zip"),
                dest_path="/born-digital/bag.zip",
                package=package
            )

        package.refresh_from_db()
        assert package.status == models.Package.FAIL

    @mock.patch("time.sleep")
    @mock.patch("locations.models.wellcome.StorageServiceClient")
    def test_tries_fetching_unknown_ingest_if_no_callback(
        self, mock_wellcome_client_class, mock_sleep
    ):
        package = models.Package.objects.get(
            uuid="6465da4a-ea88-4300-ac56-9641125f1276"
        )
        path = "locations/fixtures/bag-6465da4a-ea88-4300-ac56-9641125f1276.zip"
        package.current_path = path
        package.status = models.Package.STAGING
        package.save()

        mock_wellcome = mock_wellcome_client_class.return_value

        # This mimics the case where Archivematica gets an unknown status
        # from the Wellcome Storage, then later asks again and gets
        # a "succeeded" ingest.
        mock_wellcome.get_ingest_from_location.side_effect = [
            create_storage_service_response(status="unknown"),
            create_storage_service_response(status="succeeded"),
        ]

        self.wellcome_object.move_from_storage_service(
            src_path=os.path.join(FIXTURES_DIR, "small_compressed_bag.zip"),
            dest_path="/born-digital/bag.zip",
            package=package
        )

        package.refresh_from_db()
        assert package.status == models.Package.UPLOADED

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_raises_exception_on_ingest_failure(self, mock_wellcome_client_class, mock_sleep):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        package.status = models.Package.STAGING
        package.save()

        # Simulate the callback
        def set_package_to_fail(*args):
            package.status = models.Package.FAIL
            package.save()
        package.refresh_from_db = mock.Mock(side_effect=set_package_to_fail)

        with pytest.raises(models.StorageException):
            self.wellcome_object.move_from_storage_service(
                os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
                '/born-digital/bag.zip',
                package=package
            )


class TestWellcomeMoveToStorageService(WellcomeTestBase):

    @mock_s3
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    @mock.patch('subprocess.check_call')
    def test_copies_files_from_ia_provider(self, mock_call, mock_wellcome_client_class):
        package = self.get_package()
        package.misc_attributes["wellcome.version"] = "v3"
        package.misc_attributes["wellcome.space"] = "name-of-space"
        package.misc_attributes["wellcome.identifier"] = "bag-id"

        self._s3.create_bucket(Bucket='ia-bucket')
        self._s3.upload_fileobj(StringIO("file contents"), 'ia-bucket', 'bucket-subdir/bag-id/v3/data/file1')

        mock_wellcome = mock_wellcome_client_class.return_value
        mock_wellcome.get_bag.return_value = {
            'location': {
                'bucket': 'ia-bucket',
                'path': 'bucket-subdir/bag-id',
                'provider': {
                    'id': 'aws-s3-ia',
                }
            },
            'manifest': {
                'files': [
                    {
                        'name': 'data/file1',
                        'path': 'v3/data/file1',
                        'size': 13,
                    }
                ]
            },
            'tagManifest': {
                'files': []
            },
            'version': 'v3',
        }


        src_path = '/name-of-space/name-bag-id.tar.gz'
        dest_path = os.path.join(self.tmp_dir, 'name-bag-id.tar.gz')
        self.wellcome_object.move_to_storage_service(
            src_path,
            dest_path,
            'space-uuid',
            package=package,
        )

        mock_wellcome.get_bag.assert_called_with(space_id='name-of-space', source_id='bag-id', version='v3')
        mock_call.assert_called_with([
            'python',
            '-c',
            mock.ANY,
            mock.ANY,
            dest_path,
            'name-bag-id',
        ], stderr=subprocess.STDOUT)
        assert json.loads(mock_call.call_args[0][0][3]) == mock_wellcome.get_bag.return_value

    @mock_s3
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    @mock.patch('subprocess.check_call')
    def test_supports_path_containing_uuid(self, mock_call, mock_wellcome_client_class):
        package = self.get_package()
        package.misc_attributes["wellcome.version"] = "v3"
        package.misc_attributes["wellcome.space"] = "name-of-space"
        package.misc_attributes["wellcome.identifier"] = "bag-id"

        self._s3.create_bucket(Bucket='ia-bucket')
        self._s3.upload_fileobj(StringIO("file contents"), 'ia-bucket', 'bucket-subdir/bag-id/v3/data/file1')

        mock_wellcome = mock_wellcome_client_class.return_value
        mock_wellcome.get_bag.return_value = {
            'location': {
                'bucket': 'ia-bucket',
                'path': 'bucket-subdir/bag-id',
                'provider': {
                    'id': 'aws-s3-ia',
                },
            },
            'manifest': {
                'files': [
                    {
                        'name': 'data/file1',
                        'path': 'v3/data/file1',
                        'size': 13,
                    }
                ]
            },
            'tagManifest': {
                'files': []
            },
            'version': 'v3',
        }


        src_path = '/name-of-space/aaaa/bbbb/cccc/dddd/eeee/ffff/gggg/hhhh/name-bag-id.tar.gz'
        dest_path = os.path.join(self.tmp_dir, 'aaaa/bbbb/cccc/dddd/eeee/ffff/gggg/hhhh/name-bag-id.tar.gz')
        self.wellcome_object.move_to_storage_service(
            src_path,
            dest_path,
            'space-uuid',
            package=package,
        )

        mock_wellcome.get_bag.assert_called_with(space_id='name-of-space', source_id='bag-id', version='v3')


# TODO: It would be nice to have some end-to-end tests for this functionality.
