import botocore
import boto3
import mock
import pytest
from django.test import TestCase
from moto import mock_s3

from locations import models


@mock_s3
class TestS3Storage(TestCase):

    fixtures = ["base.json", "s3.json"]

    def setUp(self):
        self.s3_object = models.S3.objects.get(id=1)

    def test_bucket_name(self):
        assert self.s3_object.bucket_name == "test-bucket"

    def test_bucket_name_falls_back_to_space_id(self):
        self.s3_object.bucket = ""
        self.s3_object.save()

        assert self.s3_object.bucket_name == "ae37f081-8baf-4d5d-9b1f-aebe367f1707"

    def test_ensure_bucket_exists_continues_if_exists(self):
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")

        self.s3_object._ensure_bucket_exists()

        client.head_bucket(Bucket="test-bucket")

    def test_ensure_bucket_exists_creates_bucket(self):
        client = boto3.client("s3", region_name="us-east-1")

        self.s3_object._ensure_bucket_exists()

        client.head_bucket(Bucket="test-bucket")

    def test_ensure_bucket_exists_get_location_fails(self):
        self.s3_object.resource.meta.client.get_bucket_location = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )

        with pytest.raises(models.StorageException):
            self.s3_object._ensure_bucket_exists()

    def test_ensure_bucket_exists_creation_fails(self):
        self.s3_object.resource.meta.client.get_bucket_location = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )
        self.s3_object.resource.meta.client.create_bucket = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )

        with pytest.raises(models.StorageException):
            self.s3_object._ensure_bucket_exists()