import os
import shutil
import tempfile
from StringIO import StringIO

import boto3
import mock
import pytest
from django.test import TestCase
from moto import mock_s3

from locations import models


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.abspath(os.path.join(THIS_DIR, '..', 'fixtures'))


@mock_s3
class TestWellcomeMoveFromStorageService(TestCase):

    fixtures = ['base.json', 'wellcome.json']

    def setUp(self):
        self.wellcome_object = models.WellcomeStorageService.objects.get(id=1)

        self._s3 = boto3.client("s3", region_name='us-east-1')
        self._s3.create_bucket(Bucket=self.wellcome_object.s3_bucket)

    @staticmethod
    def get_package():
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        # Simulate the callback
        def set_package_to_uploaded(*args):
            package.status = models.Package.UPLOADED
            package.save()
        package.refresh_from_db = mock.Mock(side_effect=set_package_to_uploaded)
        return package

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
            ingest_type='update',
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

        mock_wellcome = mock_wellcome_client_class.return_value
        mock_wellcome.get_ingest_from_location.return_value = {
            'id': 'ingest-id',
            'callback': {
                'status': {
                    'id': 'succeeded',
                }
            },
            'status': {
                'id': 'succeeded',
            },
            'bag': {
                'info': {
                    'externalIdentifier': 'external-id',
                    'version': 'v3',
                }
            },
        }

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/born-digital/bag.zip',
            package=package
        )

        package.refresh_from_db()
        assert package.status == models.Package.UPLOADED
        assert package.current_path == 'bag-6465da4a-ea88-4300-ac56-9641125f1276.zip'
        assert package.misc_attributes['bag_id'] == 'external-id'
        assert package.misc_attributes['bag_version'] == 'v3'

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


class TestWellcomeMoveToStorageService(TestCase):
    fixtures = ['base.json', 'wellcome.json']

    def setUp(self):
        self.wellcome_object = models.WellcomeStorageService.objects.get(id=1)

        self._s3 = boto3.client("s3", region_name='us-east-1')
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)


    @mock_s3
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_copies_files_from_ia_provider(self, mock_wellcome_client_class):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        package.misc_attributes['bag_version'] = 'v3'
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


        self.wellcome_object.move_to_storage_service(
            '/name-of-space/name-bag-id.tar.gz',
            os.path.join(self.tmp_dir, 'name-bag-id.tar.gz'),
            'space-uuid',
            package=package,
        )

        mock_wellcome.get_bag.assert_called_with(space_id='name-of-space', source_id='bag-id', version='v3')
        assert os.path.exists(os.path.join(self.tmp_dir, 'name-bag-id.tar.gz'))

    @mock_s3
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_supports_path_containing_uuid(self, mock_wellcome_client_class):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        package.misc_attributes['bag_version'] = 'v3'
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
        assert os.path.exists(dest_path)
