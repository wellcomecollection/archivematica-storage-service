# stdlib, alphabetical
import errno
import logging
import os
import shutil
import stat
import subprocess

# Core Django, alphabetical
from django.core.exceptions import ValidationError
from django.db import models

# Third party dependencies, alphabetical
from django_extensions.db.fields import UUIDField

# This project, alphabetical
from common import utils
LOGGER = logging.getLogger(__name__)

# This module, alphabetical
from . import StorageException

__all__ = ('Space', )


def validate_space_path(path):
    """ Validation for path in Space.  Must be absolute. """
    if path[0] != '/':
        raise ValidationError("Path must begin with a /")

# To add a new storage space the following places must be updated:
#  locations/models/space.py (this file)
#   Add constant for storage protocol
#   Add constant to ACCESS_PROTOCOL_CHOICES
#  locations/models/<spacename>.py
#   Add class for protocol-specific fields using template below
#  locations/models/__init__.py
#   Add class to import list
#  locations/forms.py
#   Add ModelForm for new class
#  common/constants.py
#   Add entry to protocol
#    'model' is the model object
#    'form' is the ModelForm for creating the space
#    'fields' is a whitelist of fields to display to the user

# class Example(models.Model):
#     space = models.OneToOneField('Space', to_field='uuid')
#
#     class Meta:
#         verbose_name = "Example Space"
#         app_label = 'locations'
#
#     ALLOWED_LOCATION_PURPOSE = [
#         Location.AIP_RECOVERY,
#         Location.AIP_STORAGE,
#         Location.CURRENTLY_PROCESSING,
#         Location.DIP_STORAGE,
#         Location.STORAGE_SERVICE_INTERNAL,
#         Location.TRANSFER_SOURCE,
#         Location.BACKLOG,
#     ]
#
#     def browse(self, path):
#         pass
#
#     def delete_path(self, delete_path):
#         pass
#
#     def move_to_storage_service(self, src_path, dest_path, dest_space):
#         """ Moves src_path to dest_space.staging_path/dest_path. """
#         pass
#
#     def move_from_storage_service(self, source_path, destination_path):
#         """ Moves self.staging_path/src_path to dest_path. """
#         pass


class Space(models.Model):
    """ Common storage space information.

    Knows what protocol to use to access a storage space, but all protocol
    specific information is in children classes with ForeignKeys to Space."""
    uuid = UUIDField(editable=False, unique=True, version=4,
        help_text="Unique identifier")

    DURACLOUD = 'DC'
    FEDORA = 'FEDORA'
    LOCAL_FILESYSTEM = 'FS'
    LOM = 'LOM'
    NFS = 'NFS'
    PIPELINE_LOCAL_FS = 'PIPE_FS'
    OBJECT_STORAGE = {DURACLOUD}
    ACCESS_PROTOCOL_CHOICES = (
        (DURACLOUD, 'DuraCloud'),
        (FEDORA, "FEDORA via SWORD2"),
        (LOCAL_FILESYSTEM, "Local Filesystem"),
        (LOM, "LOCKSS-o-matic"),
        (NFS, "NFS"),
        (PIPELINE_LOCAL_FS, "Pipeline Local Filesystem"),
    )
    access_protocol = models.CharField(max_length=8,
        choices=ACCESS_PROTOCOL_CHOICES,
        help_text="How the space can be accessed.")
    size = models.BigIntegerField(default=None, null=True, blank=True,
        help_text="Size in bytes (optional)")
    used = models.BigIntegerField(default=0,
        help_text="Amount used in bytes")
    path = models.TextField(default='', blank=True,
        help_text="Absolute path to the space on the storage service machine.")
    staging_path = models.TextField(validators=[validate_space_path],
        help_text="Absolute path to a staging area.  Must be UNIX filesystem compatible, preferably on the same filesystem as the path.")
    verified = models.BooleanField(default=False,
       help_text="Whether or not the space has been verified to be accessible.")
    last_verified = models.DateTimeField(default=None, null=True, blank=True,
        help_text="Time this location was last verified to be accessible.")

    class Meta:
        verbose_name = 'Space'
        app_label = 'locations'

    def __unicode__(self):
        return u"{uuid}: {path} ({access_protocol})".format(
            uuid=self.uuid,
            access_protocol=self.get_access_protocol_display(),
            path=self.path,
        )

    def clean(self):
        # Object storage spaces do not require a path, or for it to start with /
        if self.access_protocol not in self.OBJECT_STORAGE:
            if not self.path:
                raise ValidationError('Path is required')
            validate_space_path(self.path)

    def get_child_space(self):
        """ Returns the protocol-specific space object. """
        # Importing PROTOCOL here because importing locations.constants at the
        # top of the file causes a circular dependency
        from ..constants import PROTOCOL
        protocol_model = PROTOCOL[self.access_protocol]['model']
        protocol_space = protocol_model.objects.get(space=self)
        # TODO try-catch AttributeError if remote_user or remote_name not exist?
        return protocol_space

    def browse(self, path, *args, **kwargs):
        """
        Return information about the objects (files, directories) at `path`.

        Attempts to call the child space's implementation.  If not found, falls
        back to looking for the path locally.

        Returns a dictionary with keys 'entries', 'directories' and 'properties'.

        'entries' is a list of strings, one for each entry in that directory, both file-like and folder-like.
        'directories' is a list of strings for each folder-like entry. Each entry should also be listed in 'entries'.
        'properties' is a dictionary that may contain additional information for the entries.  Keys are the entry name found in 'entries', values are a dictionary containing extra information. 'properties' may not contain all values from 'entries'.

        E.g.
        {
            'entries': ['BagTransfer.zip', 'Images', 'Multimedia', 'OCRImage'],
            'directories': ['Images', 'Multimedia', 'OCRImage'],
            'properties': {
                'Images': {'object count': 10},
                'Multimedia': {'object count': 7},
                'OCRImage': {'object count': 1}
            },
        }

        Values in the properties dict vary depending on the providing Space but may include:
        'size': Size of the object
        'object count': Number of objects in the directory, including children
        See each Space's browse for details.

        :param str path: Full path to return info for
        :return: Dictionary of object information detailed above.
        """
        LOGGER.info('path: %s', path)
        try:
            return self.get_child_space().browse(path, *args, **kwargs)
        except AttributeError:
            return self._browse_local(path)

    def delete_path(self, delete_path, *args, **kwargs):
        """
        Deletes `delete_path` stored in this space.

        `delete_path` is a full path in this space.

        If not implemented in the child space, looks locally.
        """
        # Enforce delete_path is in self.path
        if not delete_path.startswith(self.path):
            raise ValueError('%s is not within %s', delete_path, self.path)
        try:
            return self.get_child_space().delete_path(delete_path, *args, **kwargs)
        except AttributeError:
            return self._delete_path_local(delete_path)

    def move_to_storage_service(self, source_path, destination_path,
                                destination_space, *args, **kwargs):
        """ Move source_path to destination_path in the staging area of destination_space.

        If source_path is not an absolute path, it is assumed to be relative to
        Space.path.

        destination_path must be relative and destination_space.staging_path
        MUST be locally accessible to the storage service.

        This is implemented by the child protocol spaces.
        """
        LOGGER.debug('TO: src: %s', source_path)
        LOGGER.debug('TO: dst: %s', destination_path)
        LOGGER.debug('TO: staging: %s', destination_space.staging_path)

        # TODO enforce source_path is inside self.path
        # Path pre-processing
        source_path = os.path.join(self.path, source_path)
        # dest_path must be relative
        if os.path.isabs(destination_path):
            destination_path = destination_path.lstrip(os.sep)
            # Alternative implementation
            # os.path.join(*destination_path.split(os.sep)[1:]) # Strips up to first os.sep
        destination_path = os.path.join(destination_space.staging_path, destination_path)

        try:
            self.get_child_space().move_to_storage_service(
                source_path, destination_path, destination_space, *args, **kwargs)
        except AttributeError:
            raise NotImplementedError('{} space has not implemented move_to_storage_service'.format(self.get_access_protocol_display()))

    def post_move_to_storage_service(self, *args, **kwargs):
        """ Hook for any actions that need to be taken after moving to the storage service. """
        try:
            self.get_child_space().post_move_to_storage_service(*args, **kwargs)
        except AttributeError:
            # This is optional for the child class to implement
            pass

    def _move_from_path_mangling(self, staging_path, destination_path):
        """
        Does path pre-processing before passing to move_from_* functions.

        Given a staging_path relative to self.staging_path, converts to an absolute path.
        If staging_path is absolute (starts with /), force to be relative.
        If staging_path is a directory, ensure ends with /
        Given a destination_path relative to this space, converts to an absolute path.

        :param str staging_path: Path to the staging copy relative to the SS internal location.
        :param str destination_path: Path to the destination copy relative to this Space's path.
        :return: Tuple of absolute paths (staging_path, destination_path)
        """
        # Path pre-processing
        # source_path must be relative
        if os.path.isabs(staging_path):
            staging_path = staging_path.lstrip(os.sep)
            # Alternate implementation:
            # os.path.join(*staging_path.split(os.sep)[1:]) # Strips up to first os.sep
        staging_path = os.path.join(self.staging_path, staging_path)
        if os.path.isdir(staging_path):
            staging_path += os.sep
        destination_path = os.path.join(self.path, destination_path)

        # TODO enforce destination_path is inside self.path

        return staging_path, destination_path

    def move_from_storage_service(self, source_path, destination_path,
                                  *args, **kwargs):
        """ Move source_path in this Space's staging area to destination_path in this Space.

        That is, moves self.staging_path/source_path to self.path/destination_path.

        If destination_path is not an absolute path, it is assumed to be
        relative to Space.path.

        source_path must be relative to self.staging_path.

        This is implemented by the child protocol spaces.
        """
        LOGGER.debug('FROM: src: %s', source_path)
        LOGGER.debug('FROM: dst: %s', destination_path)

        source_path, destination_path = self._move_from_path_mangling(source_path, destination_path)
        try:
            self.get_child_space().move_from_storage_service(
                source_path, destination_path, *args, **kwargs)
        except AttributeError:
            raise NotImplementedError('{} space has not implemented move_from_storage_service'.format(self.get_access_protocol_display()))

    def post_move_from_storage_service(self, staging_path, destination_path, package=None, *args, **kwargs):
        """
        Hook for any actions that need to be taken after moving from the storage
        service to the final destination.

        :param str staging_path: Path to the staging copy relative to the SS internal location. Can be None if destination_path is also None.
        :param str destination_path: Path to the destination copy relative to this Space's path. Can be None if staging_path is also None.
        :param package: (Optional) :class:`Package` that is being moved.
        """
        if staging_path is None or destination_path is None:
            staging_path = destination_path = None
        if staging_path and destination_path:
            staging_path, destination_path = self._move_from_path_mangling(staging_path, destination_path)
        try:
            self.get_child_space().post_move_from_storage_service(
                staging_path=staging_path,
                destination_path=destination_path,
                package=package,
                *args, **kwargs)
        except AttributeError:
            # This is optional for the child class to implement
            pass
        # Delete staging copy
        if staging_path != destination_path:
            try:
                if os.path.isdir(staging_path):
                    # Need to convert this to an str - if this is a
                    # unicode string, rmtree will use os.path.join
                    # on the directory and the names of its children,
                    # which can result in an attempt to join mixed encodings;
                    # this blows up if the filename cannot be converted to
                    # unicode
                    shutil.rmtree(str(os.path.normpath(staging_path)))
                elif os.path.isfile(staging_path):
                    os.remove(os.path.normpath(staging_path))
            except OSError:
                logging.warning('Unable to remove %s', staging_path, exc_info=True)


    def update_package_status(self, package):
        """
        Check and update the status of `package` stored in this Space.
        """
        try:
            return self.get_child_space().update_package_status(package)
        except AttributeError:
            message = '{} space has not implemented update_package_status'.format(self.get_access_protocol_display())
            return (None, message)


    # HELPER FUNCTIONS

    def _move_locally(self, source_path, destination_path, mode=None):
        """ Moves a file from source_path to destination_path on the local filesystem. """
        # FIXME this does not work properly when moving folders troubleshoot
        # and fix before using.
        # When copying from folder/. to folder2/. it failed because the folder
        # already existed.  Copying folder/ or folder to folder/ or folder also
        # has errors.  Should uses shutil.move()
        LOGGER.info("Moving from %s to %s", source_path, destination_path)

        # Create directories
        self._create_local_directory(destination_path, mode)

        # Move the file
        os.rename(source_path, destination_path)

    def _move_rsync(self, source, destination):
        """ Moves a file from source to destination using rsync.

        All directories leading to destination must exist.
        Space._create_local_directory may be useful.
        """
        source = utils.coerce_str(source)
        destination = utils.coerce_str(destination)
        LOGGER.info("Rsyncing from %s to %s", source, destination)

        if source == destination:
            return

        # Rsync file over
        # TODO Do this asyncronously, with restarting failed attempts
        command = ['rsync', '-a', '--protect-args', '-vv', '--chmod=ugo+rw', '-r', source, destination]
        LOGGER.info("rsync command: %s", command)

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, _ = p.communicate()
        if p.returncode != 0:
            s = "Rsync failed with status {}: {}".format(p.returncode, stdout)
            LOGGER.warning(s)
            raise StorageException(s)

    def _create_local_directory(self, path, mode=None):
        """ Creates a local directory at 'path' with 'mode' (default 775). """
        if mode is None:
            mode = (stat.S_IRUSR + stat.S_IWUSR + stat.S_IXUSR +
                    stat.S_IRGRP + stat.S_IWGRP + stat.S_IXGRP +
                    stat.S_IROTH +                stat.S_IXOTH)
        try:
            os.makedirs(os.path.dirname(path), mode)
        except os.error as e:
            # If the leaf node already exists, that's fine
            if e.errno != errno.EEXIST:
                LOGGER.warning("Could not create storage directory: %s", e)
                raise

        # os.makedirs may ignore the mode when creating directories, so force
        # the permissions here. Some spaces (eg CIFS) doesn't allow chmod, so
        # wrap it in a try-catch and ignore the failure.
        try:
            os.chmod(os.path.dirname(path), mode)
        except os.error as e:
            LOGGER.warning(e)

    def _count_objects_in_directory(self, path):
        """
        Returns all the files in a directory, including children.
        """
        total_files = 0
        for _, _, files in os.walk(path):
            total_files += len(files)
        return total_files

    def _browse_local(self, path):
        """
        Returns browse results for a locally accessible filesystem.

        Properties provided:
        'size': Size of the object, as determined by os.path.getsize. May be misleading for directories, suggest use 'object count'
        'object count': Number of objects in the directory, including children
        """
        if isinstance(path, unicode):
            path = str(path)
        if not os.path.exists(path):
            LOGGER.info('%s in %s does not exist', path, self)
            return {'directories': [], 'entries': [], 'properties': []}
        properties = {}
        # Sorted list of all entries in directory, excluding hidden files
        entries = [name for name in os.listdir(path) if name[0] != '.']
        entries = sorted(entries, key=lambda s: s.lower())
        directories = []
        for name in entries:
            full_path = os.path.join(path, name)
            properties[name] = {'size': os.path.getsize(full_path)}
            if os.path.isdir(full_path) and os.access(full_path, os.R_OK):
                directories.append(name)
                properties[name]['object count'] = self._count_objects_in_directory(full_path)
        return {'directories': directories, 'entries': entries, 'properties': properties}

    def _delete_path_local(self, delete_path):
        """
        Deletes `delete_path` in this space, assuming it is locally accessible.
        """
        try:
            if os.path.isfile(delete_path):
                os.remove(delete_path)
            if os.path.isdir(delete_path):
                shutil.rmtree(delete_path)
        except (os.error, shutil.Error):
            LOGGER.warning("Error deleting package %s", delete_path, exc_info=True)
            raise
