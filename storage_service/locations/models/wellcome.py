import logging

import os
import boto3
from django.db import models
from django.utils.translation import ugettext_lazy as _

from . import StorageException
from .location import Location


TOKEN_HELP_TEXT = _('URL of the OAuth token endpoint, e.g. https://auth.wellcomecollection.org/oauth2/token')
API_HELP_TEXT = _('Root URL of the storage service API, e.g. https://api.wellcomecollection.org/')
LOGGER = logging.getLogger(__name__)


class WellcomeStorageService(models.Model):
    space = models.OneToOneField('Space', to_field='uuid')
    token_url = models.URLField(max_length=256, help_text=TOKEN_HELP_TEXT)
    api_root_url = models.URLField(max_length=256, help_text=API_HELP_TEXT)

    # oauth details:
    app_client_id = models.CharField(max_length=300, blank=True, null=True)
    app_client_secret = models.CharField(max_length=300, blank=True, null=True)

    # AWS config
    aws_access_key_id = models.CharField(max_length=64,
        verbose_name=_('AWS Access Key ID to authenticate'))
    aws_secret_access_key = models.CharField(max_length=256,
        verbose_name=_('AWS Secret Access Key to authenticate with'))

    aws_assumed_role = models.CharField(
        max_length=256,
        verbose_name=_('Assumed AWS IAM Role'),
        blank=True,
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

    def __init__(self, *args, **kwargs):
        super(WellcomeStorageService, self).__init__(*args, **kwargs)
        self._s3_resource = None

    @property
    def s3_resource(self):
        if self._s3_resource is None:
            sts_client = boto3.client(
                service_name='sts',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

            # TODO: handle the case where we're not assuming a role
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

        return self._s3_resource

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
            dest_path = dest_path.lstrip('/')

            with open(src_path, 'rb') as data:
                bucket.upload_fileobj(data, dest_path)
        else:
            raise StorageException(
                _('%(path)s is neither a file nor a directory, may not exist') %
                {'path': src_path})


    class Meta:
        verbose_name = _("Wellcome Storage Service")
        app_label = 'locations'


    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
    ]
