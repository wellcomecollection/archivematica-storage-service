import logging
from urlparse import urljoin

import os
import boto3
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from django.db import models
from django.utils.translation import ugettext_lazy as _

from . import StorageException
from .location import Location


TOKEN_HELP_TEXT = _('URL of the OAuth token endpoint, e.g. https://auth.wellcomecollection.org/oauth2/token')
API_HELP_TEXT = _('Root URL of the storage service API, e.g. https://api.wellcomecollection.org/')
LOGGER = logging.getLogger(__name__)


class WellcomeStorageServiceClient(object):
    """
    Client for the Wellcome Storage API
    """

    def __init__(self, api_url, oauth_details=None):
        self.api_url = api_url
        if oauth_details:
            self.session = self.oauth_session(
                oauth_details["token_url"],
                oauth_details["client_id"],
                oauth_details["client_secret"],
            )
        else:
            self.session = requests.Session()

    def oauth_session(self, token_url, client_id, client_secret):
        """
        Create a simple OAuth session
        """
        client = BackendApplicationClient(client_id=client_id)
        api_session = OAuth2Session(client=client)
        api_session.fetch_token(
            token_url=token_url, client_id=client_id, client_secret=client_secret
        )
        return api_session

    def ingest_payload(self, bag_path, ingest_bucket_name, space):
        """
        Generates an ingest bag payload.
        """
        return {
            "type": "Ingest",
            "ingestType": {"id": "create", "type": "IngestType"},
            "space": {"id": space, "type": "Space"},
            "sourceLocation": {
                "type": "Location",
                "provider": {"type": "Provider", "id": "aws-s3-standard"},
                "bucket": ingest_bucket_name,
                "path": bag_path,
            },
        }

    def ingests_endpoint(self):
        return urljoin(self.api_url, "storage/v1/ingests")

    def ingest_endpoint(self, id):
        return urljoin(self.api_url, "storage/v1/ingests/" + id)

    def bags_endpoint(self):
        return urljoin(self.api_url, "storage/v1/bags")

    def bag_endpoint(self, space, source_id):
        return urljoin(self.api_url, "storage/v1/bags/%s/%s" % (space, source_id))

    def ingest(self, bag_path, ingest_bucket_name, space):
        """
        Call the storage ingests api to ingest bags
        """
        ingests_endpoint = self.ingests_endpoint()
        response = self.session.post(
            ingests_endpoint,
            json=self.ingest_payload(bag_path, ingest_bucket_name, space),
        )
        status_code = response.status_code
        if status_code == 201:
            return response.headers.get("Location")
        else:
            raise RuntimeError("%s returned %d" % (ingests_endpoint, status_code), response)

    def get_ingest(self, ingest_id):
        """
        Call the storage ingests api to get state of an ingest
        """
        ingest_endpoint = self.ingest_endpoint(ingest_id)
        response = self.session.get(ingest_endpoint)
        status_code = response.status_code
        if status_code == 200:
            return response.json()
        else:
            raise RuntimeError("%s returned %d" % (ingests_endpoint, status_code), response)

    def get_bag(self, space, source_id):
        bag_endpoint = self.bag_endpoint(space, source_id)
        response = self.session.get(bag_endpoint)
        status_code = response.status_code
        if status_code == 200:
            return response.json()
        else:
            raise RuntimeError("%s returned %d" % (bag_endpoint, status_code), response)


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
            s3_path = dest_path.lstrip('/')

            # Upload to s3
            with open(src_path, 'rb') as data:
                bucket.upload_fileobj(data, s3_path)

            wellcome = WellcomeStorageServiceClient(self.api_root_url, {
                'token_url': self.token_url,
                'client_id': self.app_client_id,
                'client_secret': self.app_client_secret,
            })

            response = wellcome.ingest(
                s3_path,
                self.s3_bucket,
                'born-digital'
            )

            ingest_id = response.rsplit('/')[-1]
            LOGGER.info('Ingest_id: %s' % ingest_id)

            # Poll for result. TODO... hook into a callback
            import time
            LOGGER.debug('Waiting for bag...')
            while True:
                ingest = wellcome.get_ingest(ingest_id)
                status = ingest['status']['id']
                LOGGER.debug('Ingest status: %s' % status)
                if status == 'succeeded':
                    bag_id = ingest['bag']['id']
                    LOGGER.info('Bag ID: %s' % bag_id)
                    break
                elif status =='failed':
                    for event in ingest['events']:
                        LOGGER.info('{type}: {description}'.format(**event))
                    raise StorageException('AIP upload failed')
                else:
                    time.sleep(5)

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
