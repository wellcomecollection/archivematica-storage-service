import os

import botocore
import boto3
import mock
import pytest

from django.test import TestCase
from moto import mock_s3

from locations import models


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.abspath(os.path.join(THIS_DIR, "..", "fixtures"))


class TestS3Storage(TestCase):

    fixtures = ["base.json", "s3.json"]

    def setUp(self):
        self.mock = mock_s3()
        self.mock.start()
        self.s3_object = models.S3.objects.get(id=1)

    def tearDown(self):
        self.mock.stop()

    def test_bucket_name(self):
        assert self.s3_object.bucket_name == "test-bucket"

    def test_bucket_name_falls_back_to_space_id(self):
        self.s3_object.s3_bucket = ""
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

    def test_ensure_bucket_exists_head_fails(self):
        client = boto3.client("s3", region_name="us-east-1")

        self.s3_object.s3_resource.meta.client.head_bucket = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )

        with pytest.raises(models.StorageException):
            self.s3_object._ensure_bucket_exists()

    def test_ensure_bucket_exists_creation_fails(self):
        client = boto3.client("s3", region_name="us-east-1")

        self.s3_object.s3_resource.meta.client.head_bucket = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )
        self.s3_object.s3_resource.meta.client.create_bucket = mock.Mock(
            side_effect=botocore.exceptions.BotoCoreError
        )

        with pytest.raises(models.StorageException):
            self.s3_object._ensure_bucket_exists()

    def test_browse(self):
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")

        client.upload_file(
            os.path.join(FIXTURES_DIR, "working_bag.zip"),
            "test-bucket",
            "subdir/bag.zip",
        )

        contents = self.s3_object.browse("/")
        assert "subdir" in contents["entries"]
        assert "subdir" in contents["directories"]

        contents = self.s3_object.browse("/subdir")
        assert "bag.zip" in contents["entries"]
        properties = contents["properties"]["bag.zip"]
        assert "timestamp" in properties
        assert properties["e_tag"] == '"e917f867114dedf9bdb430e838da647d"'
        assert properties["size"] == 1564
