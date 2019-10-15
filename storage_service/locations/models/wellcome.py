import logging
import os

import boto3
import requests
import shutil
import subprocess
import tempfile
import time
from django.db import models
from django.core.urlresolvers import reverse
from django.utils.translation import ugettext_lazy as _
from django.utils.six.moves.urllib.parse import urljoin, urlencode
from wellcome_storage_service import StorageServiceClient

from . import StorageException
from . import Package
from .location import Location


TOKEN_HELP_TEXT = _('URL of the OAuth token endpoint, e.g. https://auth.wellcomecollection.org/oauth2/token')
API_HELP_TEXT = _('Root URL of the storage service API, e.g. https://api.wellcomecollection.org/storage/v1')
CALLBACK_HELP_TEXT = _('Publicly accessible URL of the Archivematica storage service accessible to Wellcome storage service for callback')

LOGGER = logging.getLogger(__name__)


def handle_ingest(ingest, package):
    """
    Handle an ingest json response
    """
    status = ingest['status']['id']
    if status == 'succeeded':
        bag_info = ingest['bag']['info']
        external_id = bag_info['externalIdentifier']
        bag_version = bag_info['version']
        package.status = Package.UPLOADED
        package.current_path = os.path.basename(package.current_path)
        LOGGER.debug('Saving path as %s', package.current_path)
        package.misc_attributes['ingest_id'] = ingest['id']
        package.misc_attributes['bag_version'] = bag_version
        package.save()
        LOGGER.info('Ingest ID: %s', ingest['id'])
        LOGGER.info('External ID: %s', external_id)
        LOGGER.info('Bag version: %s', bag_version)
    elif status =='failed':
        LOGGER.error('Ingest failed')
        package.status = Package.FAIL
        package.save()
        for event in ingest['events']:
            LOGGER.info('{type}: {description}'.format(**event))
    #else:
    #    LOGGER.error('Unknown ingest status %s' % status)
    #    package.status = Package.FAIL
    #    package.save()


class WellcomeStorageService(models.Model):
    space = models.OneToOneField('Space', to_field='uuid')
    token_url = models.URLField(max_length=256, help_text=TOKEN_HELP_TEXT)
    api_root_url = models.URLField(max_length=256, help_text=API_HELP_TEXT)

    # oauth details:
    app_client_id = models.CharField(max_length=300, blank=True, null=True)
    app_client_secret = models.CharField(max_length=300, blank=True, null=True)

    # AWS config
    aws_access_key_id = models.CharField(
        max_length=64,
        blank=True,
        verbose_name=_('AWS Access Key ID to authenticate'))

    aws_secret_access_key = models.CharField(
        max_length=256,
        blank=True,
        verbose_name=_('AWS Secret Access Key to authenticate with'))

    aws_assumed_role = models.CharField(
        max_length=256,
        blank=True,
        verbose_name=_('Assumed AWS IAM Role'),
    )

    s3_endpoint_url = models.CharField(max_length=2048,
        verbose_name=_('S3 Endpoint URL'),
        help_text=_('S3 Endpoint URL. Eg. https://s3.amazonaws.com'))
    s3_region = models.CharField(max_length=64,
        verbose_name=_('S3 Region'),
        help_text=_('S3 Region in S3. Eg. us-east-2'))
    s3_bucket = models.CharField(max_length=64,
        verbose_name=_('S3 Bucket'),
        help_text=_('S3 Bucket for temporary storage'))

    callback_host = models.URLField(max_length=256, help_text=CALLBACK_HELP_TEXT, blank=True)
    callback_username = models.CharField(max_length=150, blank=True)
    callback_api_key = models.CharField(max_length=256, blank=True)

    def __init__(self, *args, **kwargs):
        super(WellcomeStorageService, self).__init__(*args, **kwargs)
        self._s3_resource = None

    @property
    def s3_resource(self):
        if self._s3_resource is None:
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

                self._s3_resource = boto3.resource(
                    service_name='s3',
                    endpoint_url=self.s3_endpoint_url,
                    region_name=self.s3_region,
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken'],
                )
            else:
                self._s3_resource = boto3.resource(
                    service_name='s3',
                    endpoint_url=self.s3_endpoint_url,
                    region_name=self.s3_region,
                )

        return self._s3_resource

    def browse(self, path):
        LOGGER.debug('Browsing %s on Wellcome storage', path)
        return {
            'directories': set(),
            'entries': set(),
            'properties': {},
        }

    @property
    def wellcome_client(self):
        return StorageServiceClient(
            api_url=self.api_root_url,
            token_url=self.token_url,
            client_id=self.app_client_id,
            client_secret=self.app_client_secret,
        )

    def delete_path(self, delete_path):
        LOGGER.debug('Deleting %s from Wellcome storage', delete_path)

    def move_to_storage_service(self, src_path, dest_path, dest_space, package=None):
        """ Moves src_path to dest_space.staging_path/dest_path. """
        LOGGER.debug('Fetching %s on Wellcome storage to %s (space %s)',
            src_path, dest_path, dest_space)

        space_id, source = src_path.lstrip('/').split('/')
        filename = source
        filename, ext = source.split('.', 1)
        name, source_id = filename.split('-', 1)

        bag_kwargs = {}
        if package and 'bag_version' in package.misc_attributes:
            bag_kwargs['version'] = package.misc_attributes['bag_version']

        bag = self.wellcome_client.get_bag(space_id, source_id, **bag_kwargs)
        loc = bag['location']
        version = bag['version']
        LOGGER.debug("Fetching files from s3://%s/%s", loc['bucket'], loc['path'])
        bucket = self.s3_resource.Bucket(loc['bucket'])

        tmpdir = tempfile.mkdtemp()
        tmp_aip_dir = os.path.join(tmpdir, filename)
        # The bag is stored unzipped (i.e. as a directory tree).
        # Download all objects in the source directory to a temporary space
        s3_prefix = '%s/%s' % (loc['path'].lstrip('/'), version)
        objects = bucket.objects.filter(Prefix=s3_prefix)
        for objectSummary in objects:
            dest_file = objectSummary.key.replace(s3_prefix, tmp_aip_dir, 1)
            self.space.create_local_directory(dest_file)

            LOGGER.debug("Downloading %s", objectSummary.key)
            bucket.download_file(objectSummary.key, dest_file)

        # Now compress the temporary dir contents, writing to the path
        # Archivematica expects.
        cmd = ["/bin/tar", "cz", "-C", tmpdir, "-f", dest_path, filename]
        subprocess.call(cmd)

        shutil.rmtree(tmpdir)


    def move_from_storage_service(self, src_path, dest_path, package=None):
        """ Moves self.staging_path/src_path to dest_path. """
        LOGGER.debug('Moving %s to %s on Wellcome storage', src_path, dest_path)

        s3_temporary_path = dest_path.lstrip('/')
        bucket = self.s3_resource.Bucket(self.s3_bucket)

        if os.path.isfile(src_path):

            # Upload to s3
            with open(src_path, 'rb') as data:
                bucket.upload_fileobj(data, s3_temporary_path)

            wellcome = self.wellcome_client

            callback_url = urljoin(
                self.callback_host,
                '%s?%s' % (
                    reverse('wellcome_callback', args=['v2', 'file', package.uuid]),
                    urlencode({
                        'username': self.callback_username,
                        'api_key': self.callback_api_key,
                    })
                ))

            # Use the relative_path as the storage service space ID
            location = package.current_location
            space_id = location.relative_path.strip(os.path.sep)

            # Store name of package so it can be used on reingest
            LOGGER.debug('Path: %s', package.current_path)
            package.status = Package.STAGING
            package.save()

            LOGGER.info('Callback will be to %s', callback_url)
            location = wellcome.create_s3_ingest(
                space_id=space_id,
                s3_key=s3_temporary_path,
                s3_bucket=self.s3_bucket,
                callback_url=callback_url,
                external_identifier=package.uuid,
                ingest_type="create",
            )
            LOGGER.info('Ingest_location: %s', location)

            print('Package status %s' % package.status)
            while package.status == Package.STAGING:
                # Wait for callback to have been called
                for i in range(6):
                    package.refresh_from_db()
                    print('Package status %s' % package.status)
                    time.sleep(10)
                    if package.status != Package.STAGING:
                        break

                if package.status == Package.STAGING:
                    LOGGER.info("Callback wasn't called yet - let's check the ingest URL")

                    # It wasn't. Query the ingest URL to see if anything happened.
                    # It's possible we missed the callback (e.g. Archivematica was unavailable?)
                    # because the storage service won't retry.
                    ingest = wellcome.get_ingest_from_location(location)
                    if ingest['callback']['status']['id'] == 'processing':
                        # Just keep waiting for the callback
                        LOGGER.info("Still waiting for callback")
                    else:
                        # We missed the callback. Take results from the ingest body
                        LOGGER.info("Ingest result found")
                        handle_ingest(ingest, package)

            if package.status == Package.FAIL:
                raise StorageException(
                    _("Failed to store package %(path)s") %
                    {'path': src_path})

        else:
            raise StorageException(
                _('%(path)s is not a file, may be a directory or not exist') %
                {'path': src_path})


    class Meta:
        verbose_name = _("Wellcome Storage Service")
        app_label = 'locations'


    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
    ]
