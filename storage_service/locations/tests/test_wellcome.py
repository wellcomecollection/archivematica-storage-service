import os

import boto3
import mock
import pytest
from django.test import TestCase
from moto import mock_s3

from locations import models


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.abspath(os.path.join(THIS_DIR, '..', 'fixtures'))


@mock_s3
class TestWellcomeStorage(TestCase):

    fixtures = ['base.json', 'wellcome.json']

    def setUp(self):
        self.wellcome_object = models.WellcomeStorageService.objects.get(id=1)

        self._s3 = boto3.client("s3", region_name='us-east-1')
        self._s3.create_bucket(Bucket=self.wellcome_object.s3_bucket)

    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_move_from_ss_uploads_to_s3(self, mock_wellcome_client_class):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/ingests/bag.zip',
            package=package
        )

        assert self._s3.get_object(Bucket='ingest-bucket', Key='ingests/bag.zip')

    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_move_from_ss_uploads_to_s3(self, mock_wellcome_client_class):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/ingests/bag.zip',
            package=package
        )

        assert self._s3.get_object(Bucket='ingest-bucket', Key='ingests/bag.zip')


    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_move_from_ss_wellcome_client_calls(self, mock_wellcome_client_class):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/ingests/bag.zip',
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
            s3_key='ingests/bag.zip',
            s3_bucket=self.wellcome_object.s3_bucket,
            callback_url='https://test.localhost/api/v2/file/6465da4a-ea88-4300-ac56-9641125f1276/wellcome_callback/?username=username&api_key=api_key',
        )

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_move_from_ss_waits_for_callback(self, mock_wellcome_client_class, mock_sleep):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
        package.status = models.Package.STAGING
        package.save()

        # Simulate the callback
        def set_package_to_uploaded(*args):
            package.status = models.Package.UPLOADED
            package.save()
        package.refresh_from_db = mock.Mock(side_effect=set_package_to_uploaded)

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/ingests/bag.zip',
            package=package
        )

        assert package.refresh_from_db.call_count == 1

    @mock.patch('time.sleep')
    @mock.patch('locations.models.wellcome.StorageServiceClient')
    def test_move_from_ss_gives_up_and_tries_fetching_ingest(self, mock_wellcome_client_class, mock_sleep):
        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
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
                'id': 'bag-id',
            },
        }

        self.wellcome_object.move_from_storage_service(
            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
            '/ingests/bag.zip',
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
                '/ingests/bag.zip',
                package=package
            )
