from __future__ import absolute_import

# stdlib, alphabetical
import logging
import os

# Core Django, alphabetical
from django.db import models
from django.utils.translation import ugettext_lazy as _

# Third party dependencies, alphabetical
import botocore
import boto3
import re

# This project, alphabetical

# This module, alphabetical
from . import StorageException
from .location import Location

LOGGER = logging.getLogger(__name__)


class S3SpaceModelMixin(models.Model):

    class Meta:
        app_label = 'locations'
        abstract = True

    # Authentication details
    aws_access_key_id = models.CharField(
        max_length=64,
        blank=True,
        verbose_name=_('Access Key ID to authenticate')
    )

    aws_secret_access_key = models.CharField(
        max_length=256,
        blank=True,
        verbose_name=_('Secret Access Key to authenticate with')
    )

    aws_assumed_role = models.CharField(
        max_length=256,
        blank=True,
        verbose_name=_('Assumed AWS IAM Role')
    )

    s3_endpoint_url = models.CharField(
        max_length=2048,
        verbose_name=_('S3 Endpoint URL'),
        help_text=_('S3 Endpoint URL. Eg. https://s3.amazonaws.com')
    )

    s3_region = models.CharField(
        max_length=64,
        verbose_name=_('Region'),
        help_text=_('Region in S3. Eg. us-east-2')
    )

    s3_bucket = models.CharField(max_length=64,
        verbose_name=_('S3 Bucket'),
        blank=True,
        help_text=_('S3 Bucket Name'))

    def _ensure_bucket_exists(self):
        client = self.s3_resource.meta.client
        try:
            client.head_bucket(Bucket=self.bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response['Error']['Code']
            if error_code == '404':
                client.create_bucket(Bucket=self.bucket_name)

    @property
    def bucket_name(self):
        return self.s3_bucket or self.space_id

    @property
    def s3_resource(self):
        if not hasattr(self, '_resource'):
            if self.aws_access_key_id and self.aws_secret_access_key:

                sts_client = boto3.client(
                    service_name='sts',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )

                assumed_role = sts_client.assume_role(
                    RoleArn=self.aws_assumed_role,
                    RoleSessionName='storage-session',
                )
                credentials = assumed_role['Credentials']

                self._resource = boto3.resource(
                    service_name='s3',
                    endpoint_url=self.s3_endpoint_url,
                    region_name=self.s3_region,
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken'],
                )
            else:
                self._resource = boto3.resource(
                    service_name='s3',
                    endpoint_url=self.s3_endpoint_url,
                    region_name=self.s3_region,
                )

        return self._resource



class S3(S3SpaceModelMixin):
    space = models.OneToOneField('Space', to_field='uuid')

    class Meta(S3SpaceModelMixin.Meta):
        verbose_name = _("S3")

    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
        Location.TRANSFER_SOURCE,
    ]

    def browse(self, path):
        # strip leading slash on path
        LOGGER.debug('Browsing %s on S3 storage' % path)
        LOGGER.debug('Bucket: %s' % self.bucket_name)
        path = path.lstrip("/")

        # We need a trailing slash on non-empty prefixes because a path like:
        #
        #      /path/to/requirements
        #
        # will happily prefix match:
        #
        #      /path/to/requirements.txt
        #
        # which is not the intention!
        #
        if path != "":
            path = path.rstrip("/") + "/"

        objects = self.s3_resource.Bucket(self.bucket_name).objects.filter(Prefix=path)

        directories = set()
        entries = set()
        properties = {}

        for objectSummary in objects:
            relative_key = objectSummary.key.replace(path, "", 1).lstrip("/")

            if "/" in relative_key:
                directory_name = re.sub("/.*", "", relative_key)
                if directory_name:
                    directories.add(directory_name)
                    entries.add(directory_name)
            else:
                entries.add(relative_key)
                properties[relative_key] = {
                    "size": objectSummary.size,
                    "timestamp": objectSummary.last_modified,
                    "e_tag": objectSummary.e_tag,
                }

        return {
            "directories": list(directories),
            "entries": list(entries),
            "properties": properties,
        }

    def delete_path(self, delete_path):
        objects = self.s3_resource.Bucket(self.bucket_name).objects.filter(
            Prefix=delete_path
        )

        for objectSummary in objects:
            objectSummary.delete()

    def move_to_storage_service(self, src_path, dest_path, dest_space):
        self._ensure_bucket_exists()
        bucket = self.s3_resource.Bucket(self.bucket_name)

        # strip leading slash on src_path
        src_path = src_path.lstrip("/")

        objects = self.s3_resource.Bucket(self.bucket_name).objects.filter(
            Prefix=src_path
        )

        for objectSummary in objects:
            dest_file = objectSummary.key.replace(src_path, dest_path, 1)
            self.space.create_local_directory(dest_file)

            bucket.download_file(objectSummary.key, dest_file)

    def move_from_storage_service(self, src_path, dest_path, package=None):
        self._ensure_bucket_exists()
        bucket = self.s3_resource.Bucket(self.bucket_name)

        if os.path.isdir(src_path):
            # ensure trailing slash on both paths
            src_path = os.path.join(src_path, "")
            dest_path = os.path.join(dest_path, "")

            # strip leading slash on dest_path
            dest_path = dest_path.lstrip("/")

            for path, dirs, files in os.walk(src_path):
                for basename in files:
                    entry = os.path.join(path, basename)
                    dest = entry.replace(src_path, dest_path, 1)

                    with open(entry, "rb") as data:
                        bucket.upload_fileobj(data, dest)

        elif os.path.isfile(src_path):
            # strip leading slash on dest_path
            dest_path = dest_path.lstrip("/")

            with open(src_path, "rb") as data:
                bucket.upload_fileobj(data, dest_path)

        else:
            raise StorageException(
                _("%(path)s is neither a file nor a directory, may not exist")
                % {"path": src_path}
            )
