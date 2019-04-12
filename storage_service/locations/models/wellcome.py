import logging

import os
import boto3
import requests
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


def handle_ingest(ingest, package):
    """
    Handle an ingest json response
    """
    status = ingest['status']['id']
    if status == 'succeeded':
        bag_id = ingest['bag']['id']
        package.status = Package.UPLOADED
        package.misc_attributes['bag_id'] = bag_id
        package.misc_attributes['ingest_id'] = ingest['id']
        package.current_path = bag_id
        package.save()
        LOGGER.info('Bag ID: %s' % bag_id)
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
        LOGGER.debug('Browsing %s on Wellcome storage' % path)
        return {
            'directories': set(),
            'entries': set(),
            'properties': {},
        }

    def delete_path(self, delete_path):
        LOGGER.debug('Deleting %s from Wellcome storage' % delete_path)

    def move_to_storage_service(self, src_path, dest_path, dest_space):
        """ Moves src_path to dest_space.staging_path/dest_path. """
        LOGGER.debug('Fetching %s from %s (%s) on Wellcome storage' % (
            src_path, dest_path, dest_space))

    def move_from_storage_service(self, src_path, dest_path, package=None):
        """ Moves self.staging_path/src_path to dest_path. """
        LOGGER.debug('Moving %s to %s on Wellcome storage' % (src_path, dest_path))

        bucket = self.s3_resource.Bucket(self.s3_bucket)

        if os.path.isfile(src_path):
            # strip leading slash on dest_path
            s3_path = dest_path.lstrip('/')

            # Upload to s3
            with open(src_path, 'rb') as data:
                bucket.upload_fileobj(data, s3_path)

            wellcome = StorageServiceClient(
                api_url=self.api_root_url,
                token_url=self.token_url,
                client_id=self.app_client_id,
                client_secret=self.app_client_secret,
            )

            callback_url = urljoin(
                self.callback_host,
                '%s?%s' % (
                    reverse('wellcome_callback', args=['v2', 'file', package.uuid]),
                    urlencode({
                        'username': self.callback_username,
                        'api_key': self.callback_api_key,
                    })
                ))

            LOGGER.info('Callback will be to %s' % callback_url)
            location = wellcome.create_s3_ingest(
                space_id='born-digital',
                s3_key=s3_path,
                s3_bucket=self.bucket_name,
                callback_url=callback_url,
            )
            LOGGER.info('Ingest_location: %s' % location)

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


    class Meta(S3SpaceModelMixin.Meta):
        verbose_name = _("Wellcome Storage Service")


    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
    ]
