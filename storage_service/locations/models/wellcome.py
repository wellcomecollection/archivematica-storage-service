import logging

import errno
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
from .s3 import S3SpaceModelMixin


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

download_compressed_bag(storage_manifest=bag, out_path=dest_path)
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


class WellcomeStorageService(S3SpaceModelMixin):
    space = models.OneToOneField('Space', to_field='uuid')
    token_url = models.URLField(max_length=256, help_text=TOKEN_HELP_TEXT)
    api_root_url = models.URLField(max_length=256, help_text=API_HELP_TEXT)

    # oauth details:
    app_client_id = models.CharField(max_length=300, blank=True, null=True)
    app_client_secret = models.CharField(max_length=300, blank=True, null=True)

    callback_host = models.URLField(max_length=256, help_text=CALLBACK_HELP_TEXT, blank=True)
    callback_username = models.CharField(max_length=150, blank=True)
    callback_api_key = models.CharField(max_length=256, blank=True)

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
        """
        Download an AIP from Wellcome Storage to Archivematica.
        """
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
            dest_path
        ], stderr=subprocess.STDOUT)

    def move_from_storage_service(self, src_path, dest_path, package=None):
        """
        Upload an AIP from Archivematica to the Wellcome Storage.
        """
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
                    urlencode([
                        ("username", self.callback_username),
                        ("api_key", self.callback_api_key),
                    ])
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
                s3_bucket=self.bucket_name,
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


    class Meta(S3SpaceModelMixin.Meta):
        verbose_name = _("Wellcome Storage Service")


    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
    ]
