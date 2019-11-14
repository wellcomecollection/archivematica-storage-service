import os
from StringIO import StringIO

import mock
import pytest

from metsrw import FSEntry

from common import utils

# Until further work is done to bring compression into its own module we can
# use these constants for this test, but we can do better.
PROG_VERS_7Z = "7z"
PROG_VERS_TAR = "tar"

# Specifically string types for the tuple we create.
COMPRESS_ORDER_ONE = "1"
COMPRESS_ORDER_TWO = "2"


@pytest.mark.parametrize(
    "pronom,algorithm,compression",
    [
        (utils.PRONOM_7Z, utils.COMPRESS_ALGO_BZIP2, utils.COMPRESSION_7Z_BZIP),
        (utils.PRONOM_7Z, utils.COMPRESS_ALGO_LZMA, utils.COMPRESSION_7Z_LZMA),
        (utils.PRONOM_7Z, utils.COMPRESS_ALGO_7Z_COPY, utils.COMPRESSION_7Z_COPY),
        (utils.PRONOM_7Z, "unknown algo", utils.COMPRESSION_7Z_BZIP),
        (utils.PRONOM_BZIP2, "", utils.COMPRESSION_TAR_BZIP2),
        (utils.PRONOM_GZIP, "", utils.COMPRESSION_TAR_GZIP),
        ("unknown pronom", "", utils.COMPRESSION_7Z_BZIP),
    ],
)
def test_get_compression(pronom, algorithm, compression):
    xml = (
        '<?xml version="1.0"?>'
        '<mets:mets xmlns:mets="http://www.loc.gov/METS/" xmlns:premis="info:lc/xmlns/premis-v2">'
        " <premis:formatRegistryKey>%s</premis:formatRegistryKey>"
        ' <mets:transformFile TRANSFORMALGORITHM="%s"></mets:transformFile>'
        "</mets:mets>"
    ) % (pronom, algorithm)

    assert (
        utils.get_compression(StringIO(xml)) == compression
    ), "Incorrect compression value: {} returned for XML (pointer file) input".format(
        compression
    )


@pytest.mark.parametrize(
    "compression,command",
    [
        (
            utils.COMPRESSION_7Z_BZIP,
            "7z a -bd -t7z -y -m0=bzip2 -mtc=on -mtm=on -mta=on -mmt=on /extract/filename.7z /full/path",
        ),
        (
            utils.COMPRESSION_7Z_LZMA,
            "7z a -bd -t7z -y -m0=lzma -mtc=on -mtm=on -mta=on -mmt=on /extract/filename.7z /full/path",
        ),
        (
            utils.COMPRESSION_7Z_COPY,
            "7z a -bd -t7z -y -m0=copy -mtc=on -mtm=on -mta=on -mmt=on /extract/filename.7z /full/path",
        ),
        (utils.COMPRESSION_TAR, "tar c -C /full -f /extract/filename.tar path"),
        (
            utils.COMPRESSION_TAR_GZIP,
            "tar c -z -C /full -f /extract/filename.tar.gz path",
        ),
        (
            utils.COMPRESSION_TAR_BZIP2,
            "tar c -j -C /full -f /extract/filename.tar.bz2 path",
        ),
    ],
)
def test_get_compress_command(compression, command):
    cmd, _ = utils.get_compress_command(
        compression, "/extract/", "filename", "/full/path"
    )
    assert (
        " ".join(cmd) == command
    ), "Incorrect compression command: {0} returned for compression input {1}".format(
        cmd, compression
    )


@pytest.mark.parametrize(
    "compression,command",
    [
        (
            utils.COMPRESSION_7Z_BZIP,
            '#!/bin/bash\necho program="7z"\\; algorithm="bzip2"\\; version="`7z | grep Version`"',
        ),
        (
            utils.COMPRESSION_7Z_LZMA,
            '#!/bin/bash\necho program="7z"\\; algorithm="lzma"\\; version="`7z | grep Version`"',
        ),
        (
            utils.COMPRESSION_7Z_COPY,
            '#!/bin/bash\necho program="7z"\\; algorithm="copy"\\; version="`7z | grep Version`"',
        ),
        (
            utils.COMPRESSION_TAR,
            'echo program="tar"\\; algorithm=""\\; version="`tar --version | grep tar`"',
        ),
        (
            utils.COMPRESSION_TAR_GZIP,
            'echo program="tar"\\; algorithm="-z"\\; version="`tar --version | grep tar`"',
        ),
        (
            utils.COMPRESSION_TAR_BZIP2,
            'echo program="tar"\\; algorithm="-j"\\; version="`tar --version | grep tar`"',
        ),
    ],
)
def test_get_tool_info_command(compression, command):
    cmd = utils.get_tool_info_command(compression)
    assert (
        cmd == command
    ), "Incorrect tool info: {0} returned for compression input {1}".format(
        cmd, compression
    )


@pytest.mark.parametrize(
    "compression,cmd_output,expected_detail",
    [
        (
            utils.COMPRESSION_7Z_BZIP,
            "7z command\nVersion 3.0\nsomething else",
            'program="7z"; version="Version 3.0"',
        ),
        (
            utils.COMPRESSION_7Z_LZMA,
            "7z command\nVersion 3.0\nsomething else",
            'program="7z"; version="Version 3.0"',
        ),
        (
            utils.COMPRESSION_7Z_COPY,
            "7z command\nVersion 3.0\nsomething else",
            'program="7z"; version="Version 3.0"',
        ),
        (
            utils.COMPRESSION_TAR,
            "tar version 2.0",
            'program="tar"; version="tar version 2.0"',
        ),
        (
            utils.COMPRESSION_TAR_GZIP,
            "tar version 2.0",
            'program="tar"; version="tar version 2.0"',
        ),
        (
            utils.COMPRESSION_TAR_BZIP2,
            "tar version 2.0",
            'program="tar"; version="tar version 2.0"',
        ),
    ],
)
@mock.patch("subprocess.check_output")
def test_get_compression_event_detail(
    mock_subprocess, compression, cmd_output, expected_detail
):
    mock_subprocess.return_value = cmd_output
    detail = utils.get_compression_event_detail(compression)

    assert (
        detail == expected_detail
    ), "Incorrect detail: {0} returned for compression input {1}".format(
        detail, compression
    )


@pytest.mark.parametrize(
    "compression, version,extension,program_name,transform",
    [
        (
            utils.COMPRESSION_7Z_BZIP,
            PROG_VERS_7Z,
            utils.COMPRESS_EXTENSION_7Z,
            utils.COMPRESS_PROGRAM_7Z,
            [
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_ONE,
                    "algorithm": utils.COMPRESS_ALGO_BZIP2,
                }
            ],
        ),
        (
            utils.COMPRESSION_7Z_LZMA,
            PROG_VERS_7Z,
            utils.COMPRESS_EXTENSION_7Z,
            utils.COMPRESS_PROGRAM_7Z,
            [
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_ONE,
                    "algorithm": utils.COMPRESS_ALGO_LZMA,
                }
            ],
        ),
        (
            utils.COMPRESSION_7Z_COPY,
            PROG_VERS_7Z,
            utils.COMPRESS_EXTENSION_7Z,
            utils.COMPRESS_PROGRAM_7Z,
            [
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_ONE,
                    "algorithm": utils.COMPRESS_ALGO_7Z_COPY,
                }
            ],
        ),
        (
            utils.COMPRESSION_TAR_BZIP2,
            PROG_VERS_TAR,
            utils.COMPRESS_EXTENSION_BZIP2,
            utils.COMPRESS_PROGRAM_TAR,
            [
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_ONE,
                    "algorithm": utils.COMPRESS_ALGO_BZIP2,
                },
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_TWO,
                    "algorithm": utils.COMPRESS_ALGO_TAR,
                },
            ],
        ),
        (
            utils.COMPRESSION_TAR_GZIP,
            PROG_VERS_TAR,
            utils.COMPRESS_EXTENSION_GZIP,
            utils.COMPRESS_PROGRAM_TAR,
            [
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_ONE,
                    "algorithm": utils.COMPRESS_ALGO_GZIP,
                },
                {
                    "type": utils.DECOMPRESS_TRANSFORM_TYPE,
                    "order": COMPRESS_ORDER_TWO,
                    "algorithm": utils.COMPRESS_ALGO_TAR,
                },
            ],
        ),
    ],
)
def test_get_format_info(compression, version, extension, program_name, transform):
    """Ensure that the format information we write per compression is
    consistent.
    """
    fsentry = FSEntry()
    vers, ext, prog_name = utils.get_compression_transforms(fsentry, compression, 1)
    assert version in vers
    assert ext == extension
    assert program_name in prog_name
    assert fsentry.transform_files == transform


@pytest.mark.parametrize("name, directories", [
    # An archive created from three nested folders and a single file
    ("a.tar.gz", ["a", "a/b", "a/b/c"]),

    # An archive that contains a single text file
    ("hello.txt.tbz", []),
])
def test_list_archive_directories(name, directories):
    path = os.path.abspath(os.path.join("files", name))
    assert utils.list_archive_directories(path) == directories


@pytest.mark.parametrize("name, base_directory", [
    # An archive created from three nested folders and a single file
    ("a.tar.gz", "a"),
])
def test_get_base_directory(name, base_directory):
    path = os.path.abspath(os.path.join("files", name))
    assert utils.get_base_directory(path) == base_directory


def test_get_base_directory_when_no_directory():
    path = os.path.abspath(os.path.join("files", "hello.txt.tbz"))

    with pytest.raises(ValueError, match="Could not find base directory"):
        utils.get_base_directory(path)
