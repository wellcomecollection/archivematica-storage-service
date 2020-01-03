import logging

import errno
import os
import json
import subprocess
import tempfile
import time

import bagit
from django.db import models
from django.core.urlresolvers import reverse
from django.utils.translation import ugettext_lazy as _
from django.utils.six.moves.urllib.parse import urljoin, urlencode
from lxml import etree
from wellcome_storage_service import BagNotFound, StorageServiceClient

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
        package.misc_attributes["wellcome.version"] = bag_info["version"]

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


def mkdir_p(dirpath):
    """Create a directory, even if it already exists.

    When Archivematica is running exclusively in Python 3, calls to this function
    can be replaced with ``os.makedirs(dirpath, exist_ok=True)``.

    """
    try:
        os.makedirs(dirpath)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(dirpath):
            pass
        else:
            raise


def get_wellcome_identifier(src_path, package_uuid):
    """
    By default, Archivematica will use the UUID as the External-Identifier
    when calling the Wellcome Storage.

    This is somewhat unpleasant -- if you're browsing the storage without
    Archivematica references, it's hard to know where to find a given archive.
    For example, if you're looking for PPMIA/1/2, what UUID is that?

    If all the objects in the bag have a common value in the dc.identifier field,
    which should be a catalogue reference, use that in preference to the
    Archivematica external identifier.

    """
    LOGGER.debug("Trying to find Wellcome identifier in %s", src_path)

    # If we're not looking at a tar.gz compressed bag, stop.
    if not src_path.endswith(".tar.gz"):
        return package_uuid

    # Unpack the tar.gz to a temporary directory.  We run tar in a subprocess
    # because it's CPU intensive and we don't want to hang the main Archivematica
    # thread.
    temp_dir = tempfile.mkdtemp()
    try:
        subprocess.check_call(
            ["tar", "-xzf", src_path, "-C", temp_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError as err:
        LOGGER.debug("Error uncompressing tar.gz bag: %r", err)
        return package_uuid

    # There should be a single directory in the temporary directory -- the
    # uncompressed bag.
    if len(os.listdir(temp_dir)) != 1:
        LOGGER.debug(
            "Unable to identify root of bag in: os.listdir(%r) = %r",
            temp_dir, os.listdir(temp_dir)
        )
        return package_uuid

    # Inside the bag, we look for the METS.xml file that contains information
    # about the package.  If we can't find it unambiguously, give up.
    bag_dir = os.path.join(temp_dir, os.listdir(temp_dir)[0])
    assert os.path.exists(bag_dir)
    LOGGER.debug("Expanded bag into directory %s" % bag_dir)
    bag = bagit.Bag(bag_dir)

    mets_files = [
        name
        for name in bag.payload_files()
        if name == "data/METS.%s.xml" % package_uuid
    ]

    if len(mets_files) != 1:
        LOGGER.debug("Unable to find METS file in bag: %r" % mets_files)
        return package_uuid

    # Now we know we can unpack the bag, and we've found the METS file.
    # Parse the METS file.
    mets_path = os.path.join(bag_dir, mets_files[0])
    tree = etree.parse(mets_path)

    # Try to get some identifiers from the METS file.  We try to use the
    # Dublin Core identifiers first, if not the accession number, and if
    # both of those fail we fall back to the package UUID.
    try:
        LOGGER.debug("Looking for Dublin-Core identifiers in the METS")
        wellcome_identifier = get_common_prefix(
            extract_dc_identifiers(tree)
        )
    except NoCommonPrefix as err:
        LOGGER.debug("No common prefix in the Dublin-Core identifiers")
        LOGGER.debug("Looking for accession numbers in the METS")
        try:
            wellcome_identifier = get_common_prefix(
                extract_accession_identifiers(tree)
            )
        except NoCommonPrefix:
            LOGGER.debug(
                "No common prefix in the accession numbers, falling back to UUID")
            return

    # To make the bag layout a bit more deterministic, rename the
    # METS file to something that doesn't include the package UUID.
    bare_mets_path = os.path.join(bag_dir, "data", "METS.xml")
    if (
        not os.path.exists(bare_mets_path) or
        not mets_path.startswith(os.path.join(bag_dir, "data"))
    ):
        os.rename(mets_path, bare_mets_path)

    # At this point, we've found a common prefix and it's non-empty.
    # Write it back into the bag, then compress the bag back up under
    # the original path.  Remember to rebuild the manifests.
    bag.info["External-Identifier"] = wellcome_identifier
    bag.save(manifests=True)
    LOGGER.debug("Detected Wellcome identifier as %s", wellcome_identifier)

    # Recompress the bag.  We write it to a temporary path first, so if we
    # corrupt something, the original tar.gz is preserved.
    try:
        subprocess.check_call([
            "tar",

            # Compress to /src_path.tmp using gzip compression (-z)
            "-czvf", src_path + ".tmp",

            # cd into temp_dir first, then compress everything it contains.
            # This means all the files in the tar.gz are relative, not
            # absolute paths to /tmp/...
            "-C", temp_dir, "."
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as err:
        LOGGER.debug("Error repacking bag as tar.gz: %r" % err)

    # This rename should be atomic.
    os.rename(src_path + ".tmp", src_path)
    return wellcome_identifier


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
        mkdir_p(dest_dir)

        assert package is not None

        space_id = package.misc_attributes["wellcome.space"]
        source_id = package.misc_attributes["wellcome.identifier"]
        version = package.misc_attributes.get("wellcome.version")

        # Look up the bag details by UUID
        bag = self.wellcome_client.get_bag(
            space_id=space_id,
            source_id=source_id,
            version=version
        )

        src_filename = os.path.basename(src_path)
        src_name, _ = src_filename.split(".", 1)

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
            src_name,
        ], stderr=subprocess.STDOUT)

    def move_from_storage_service(self, src_path, dest_path, package=None):
        """
        Upload an AIP from Archivematica to the Wellcome Storage.
        """
        LOGGER.debug('Moving %s to %s on Wellcome storage', src_path, dest_path)

        s3_temporary_path = dest_path.lstrip('/')
        bucket = self.s3_resource.Bucket(self.s3_bucket)

        # The src_path to the package is typically a string of the form
        #
        #     /u/u/i/d/{sip_name}-{uuid}.tar.gz
        #
        # The {sip_name} is a human-readable identifier -- if we can use that,
        # it better corresponds to the catalogue records.
        #
        # See if we can extract it, and if not, fall back to the UUID.
        src_filename = os.path.basename(src_path)
        src_name, __ = os.path.splitext(src_filename)

        wellcome_identifier = get_wellcome_identifier(
            src_path=src_path,
            package_uuid=package.uuid
        )

        # The Wellcome Storage reads packages out of S3, so we need to
        # upload the AIP to S3 before asking the WS to ingest it.
        #
        # We have to upload to S3 *after* calling get_wellcome_identifier,
        # because that might modify the External-Identifier in the bag-info.txt.
        try:
            with open(src_path, "rb") as data:
                bucket.upload_fileobj(data, s3_temporary_path)
        except Exception as err:
            LOGGER.warn("Error uploading %s to S3: %r", src_path, err)
            raise StorageException(
                _('%(path)s is not a file, may be a directory or not exist') %
                {'path': src_path})

        # Use the relative_path as the storage service space ID
        location = package.current_location
        space_id = location.relative_path.strip(os.path.sep)

        # We don't know if other packages have been ingested to the
        # Wellcome Storage for this identifier -- query for existing bags,
        # and select an ingest type appropriately.
        if wellcome_identifier == package.uuid:
            ingest_type = "create"
        else:
            try:
                self.wellcome_client.get_bag(
                    space_id=space_id,
                    source_id=wellcome_identifier
                )
            except BagNotFound:
                ingest_type = "create"
            else:
                ingest_type = "update"

        # Construct a callback URL that the storage service can use to
        # notify Archivematica of a completed ingest.
        # TODO: Don't embed raw API credentials.
        # See https://github.com/wellcometrust/platform/issues/3534
        callback_url = urljoin(
            self.callback_host,
            '%s?%s' % (
                reverse('wellcome_callback', args=['v2', 'file', package.uuid]),
                urlencode([
                    ("username", self.callback_username),
                    ("api_key", self.callback_api_key),
                ])
            ))

        # Record the attributes on the package, so we can use them to
        # retrieve a bag later.
        package.misc_attributes["wellcome.identifier"] = wellcome_identifier
        package.misc_attributes["wellcome.space"] = space_id

        LOGGER.info(
            "Uploading to Wellcome Storage with external identifier %s, space %s, ingest type %s",
            wellcome_identifier, space_id, ingest_type
        )

        # For reingests, the package status will still be 'uploaded'
        # We use the status to detect when upload is complete,
        # so it is explicitly reset here.
        package.status = Package.STAGING
        package.save()

        # Either create or update a bag on the storage service
        # https://github.com/wellcometrust/platform/tree/master/docs/rfcs/002-archival_storage#updating-an-existing-bag
        LOGGER.info('Callback will be to %s', callback_url)
        location = self.wellcome_client.create_s3_ingest(
            space_id=space_id,
            s3_key=s3_temporary_path,
            s3_bucket=self.bucket_name,
            callback_url=callback_url,
            external_identifier=wellcome_identifier,
            ingest_type=ingest_type,
        )
        LOGGER.info('Ingest_location: %s', location)

        LOGGER.debug('Current package status is %s', package.status)
        while package.status == Package.STAGING:
            # Wait for callback to have been called
            for i in range(6):
                package.refresh_from_db()
                LOGGER.debug('Polled package; status is %s', package.status)
                time.sleep(10)
                if package.status != Package.STAGING:
                    break

            if package.status == Package.STAGING:
                LOGGER.info("Callback wasn't called yet - let's check the ingest URL")

                # It wasn't. Query the ingest URL to see if anything happened.
                # It's possible we missed the callback (e.g. Archivematica was unavailable?)
                # because the storage service won't retry.
                ingest = self.wellcome_client.get_ingest_from_location(location)
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

    class Meta(S3SpaceModelMixin.Meta):
        verbose_name = _("Wellcome Storage Service")

    ALLOWED_LOCATION_PURPOSE = [
        Location.AIP_STORAGE,
    ]


class NoCommonPrefix(Exception):
    pass


def get_common_prefix(identifiers):
    """
    A METS file may contain an identifier at multiple points.  We want to pick
    a common prefix, if any, to use as the external identifier in the
    Wellcome Archival Storage.

    e.g. get_common_prefix(["AA/1", "AA/2"]) == "AA"

    """
    split_identifiers = [ident.split("/") for ident in identifiers]
    common_components = os.path.commonprefix(split_identifiers)

    if not common_components:
        raise NoCommonPrefix()

    # If the last element is an empty string, remove it -- this avoids
    # getting an identifier that ends with a /.
    while common_components[-1] == "":
        common_components = common_components[:-1]

    return "/".join(common_components)


def extract_dc_identifiers(tree):
    """
    Find all Dublin-Core identifier values in a METS file.
    """
    # The Dublin-Core identifiers are typically in a block a bit like:
    #
    #       <mets:dmdSec ID="dmdSec_2">
    #         <mets:mdWrap MDTYPE="DC">
    #           <mets:xmlData>
    #             <dcterms:dublincore xmlns:dc="http://purl.org/dc/elements/1.1/" ...>
    #               <dc:identifier>ID/1/2/3/4</dc:identifier>
    #             </dcterms:dublincore>
    #           </mets:xmlData>
    #         </mets:mdWrap>
    #       </mets:dmdSec>
    #
    # So we look for instances of "identifier" in the "dc:" namespace.
    #
    namespaces = {
        "mets": "http://www.loc.gov/METS/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterms": "http://purl.org/dc/terms/",
    }
    for identifier in tree.xpath(".//dc:identifier", namespaces=namespaces):
        yield identifier.text


def extract_accession_identifiers(tree):
    """
    Find all accession identifiers in the METS files.
    """
    # The Accession identifier is written into a block of exiftool data,
    # of the form:
    #
    #       <exiftool xmlns="">
    #         ...
    #         <MetsMetsHdrAltRecordIDType>Accession ID</MetsMetsHdrAltRecordIDType>
    #         <MetsMetsHdrAltRecordID>LEMON/ALEX</MetsMetsHdrAltRecordID>
    #         ...
    #       </exiftool>
    #
    # So we look for instances of MetsMetsHdrAltRecordID that follow an instance
    # of MetsMetsHdrAltRecordIDType with value "Accession ID".
    #
    # In practice, I haven't seen Archivematica write any METS that features an
    # MetsMetsHdrAltRecordIDType with anything but "Accession ID", but this
    # should be more robust against possible future changes.
    #
    # Breaking down the XPath:
    #
    #     .//MetsMetsHdrAltRecordIDType   Get tags of type <MetsMetsHdrAltRecordIDType>
    #     [text()='Accession ID']         Only if the text is 'Accession ID'
    #     /following                      Get tags immediately following this one
    #     ::MetsMetsHdrAltRecordID        Only if the type is <MetsMetsHdrAltRecordID>
    #
    for record in tree.xpath(".//MetsMetsHdrAltRecordIDType[text()='Accession ID']/following::MetsMetsHdrAltRecordID"):
        yield record.text
