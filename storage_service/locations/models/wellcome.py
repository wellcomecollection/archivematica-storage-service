import errno
import logging
import os

import boto3
import json
import requests
import shutil
import subprocess
import tarfile
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


# The script we use to download a compressed bag from S3.
# This is run in a subprocess.
DOWNLOAD_BAG_SCRIPT = '''
import json, sys
from wellcome_storage_service import download_compressed_bag

bag = json.loads(sys.argv[1])
dest_path = sys.argv[2]
top_level_dir=sys.argv[3]

download_compressed_bag(storage_manifest=bag, out_path=dest_path, top_level_dir=top_level_dir)
'''

def handle_ingest(ingest, package):
    """
    Handle an ingest json response
    """
    status = ingest['status']['id']
    if status == 'succeeded':
        package.status = Package.UPLOADED
        # Strip the directory context from the package path so it is
        # in the format NAME-uuid.tar.gz
        package.current_path = os.path.basename(package.current_path)
        bag_info = ingest['bag']['info']
        package.misc_attributes['bag_id'] = bag_info['externalIdentifier']
        package.misc_attributes['bag_version'] = bag_info['version']

        LOGGER.debug('Package path: %s', package.current_path)
        LOGGER.debug('Package attributes: %s', package.misc_attributes)

        package.save()

    elif status =='failed':
        LOGGER.error('Ingest failed')
        package.status = Package.FAIL
        package.save()
        for event in ingest['events']:
            LOGGER.info('{type}: {description}'.format(**event))

    else:
        LOGGER.info("Unrecognised package status: %s", status)


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

        # Ensure the target directory exists. This is where the tarball
        # will be created.
        dest_dir = os.path.dirname(dest_path)
        try:
            os.makedirs(dest_dir)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(dest_dir):
                pass
            else:
                raise

        # Possible formats for the path
        #   /space-id/NAME-uuid.tar.gz
        #   /space-id/u/u/i/d/NAME-uuid-tar.gz (this happens on reingest)
        components = src_path.lstrip('/').split('/')
        space_id, source = components[0], components[-1]
        filename, ext = source.split('.', 1)
        name, source_id = filename.split('-', 1)

        # Request a specific bag version if the package has one
        bag_kwargs = {
            'space_id': space_id,
            'source_id': source_id,
        }
        if package and 'bag_version' in package.misc_attributes:
            bag_kwargs['version'] = package.misc_attributes['bag_version']

        # Look up the bag details by UUID
        bag = self.wellcome_client.get_bag(**bag_kwargs)

        # We use a subprocess here because the compression of the download
        # is CPU-intensive, especially for larger files, and can render the
        # main process unresponsive. This can cause problems (a) for server
        # responsiveness (b) because the server may stop responding to health
        # checks, resulting in it being terminated before it can finish
        # building the archive.
        # See https://github.com/wellcometrust/platform/issues/3954
        subprocess.check_call([
            'python',
            '-c', DOWNLOAD_BAG_SCRIPT,
            json.dumps(bag),
            dest_path,
            filename,
        ], stderr=subprocess.STDOUT)

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

            # For reingests, the package status will still be 'uploaded'
            # We use the status to detect when upload is complete,
            # so it is explicitly reset here.
            package.status = Package.STAGING
            package.save()

            # Either create or update a bag on the storage service
            # https://github.com/wellcometrust/platform/tree/master/docs/rfcs/002-archival_storage#updating-an-existing-bag
            is_reingest = 'bag_id' in package.misc_attributes
            LOGGER.info('Callback will be to %s', callback_url)
            location = wellcome.create_s3_ingest(
                space_id=space_id,
                s3_key=s3_temporary_path,
                s3_bucket=self.s3_bucket,
                callback_url=callback_url,
                external_identifier=package.uuid,
                ingest_type="update" if is_reingest else "create",
            )
            LOGGER.info('Ingest_location: %s', location)

            LOGGER.debug('Package status %s', package.status)
            while package.status == Package.STAGING:
                # Wait for callback to have been called
                for i in range(6):
                    package.refresh_from_db()
                    LOGGER.debug('Package status %s', package.status)
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
