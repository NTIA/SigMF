# Copyright 2017 GNU Radio Foundation
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Create and extract SigMF archives."""

from __future__ import absolute_import

import codecs
import collections
import os
import shutil
import tarfile
import tempfile

import json

from . import error


SIGMF_ARCHIVE_EXT = ".sigmf"
SIGMF_METADATA_EXT = ".sigmf-meta"
SIGMF_DATASET_EXT = ".sigmf-data"


class SigMFArchive(object):
    """Archive one or more `SigMFFile`s.

    A `.sigmf` file must include both valid metadata and data. If metadata
    is not valid, raise `SigMFValidationError`. If `self.data_file` is not
    set or the requested output file is not writable, raise `SigMFFileError`.

    Parameters:

      sigmffiles -- An iterable of SigMFFiles.

      path       -- path to archive file to create. If file exists, overwrite.
                    If `path` doesn't end in .sigmf, it will be appended. The
                    `self.path` instance variable will be updated upon
                    successful writing of the archive to point to the final
                    archive path.

      fileobj    -- If `fileobj` is specified, it is used as an alternative to
                    a file object opened in binary mode for `path`. If
                    `fileobj` is an open tarfile, it will be appended to. It is
                    supposed to be at position 0. `fileobj` won't be closed. If
                    `fileobj` is given, `path` has no effect.

    """
    def __init__(self, sigmffiles, path=None, fileobj=None):
        self.path = path
        self.fileobj = fileobj

        if isinstance(sigmffiles, collections.Iterable):
            self.sigmffiles = sigmffiles
        else:
            self.sigmffiles = [sigmffiles]

        self._check_input()

        mode = "a" if fileobj is not None else "w"
        sigmf_fileobj = self._get_output_fileobj()
        try:
            sigmf_archive = tarfile.TarFile(mode=mode,
                                            fileobj=sigmf_fileobj,
                                            format=tarfile.PAX_FORMAT)
        except tarfile.ReadError:
            # fileobj doesn't contain any archives yet, so reopen in 'w' mode
            sigmf_archive = tarfile.TarFile(mode='w',
                                            fileobj=sigmf_fileobj,
                                            format=tarfile.PAX_FORMAT)

        def chmod(tarinfo):
            if tarinfo.isdir():
                tarinfo.mode = 0o755  # dwrxw-rw-r
            else:
                tarinfo.mode = 0o644  # -wr-r--r--
            return tarinfo

        for sigmffile in self.sigmffiles:
            tmpdir = tempfile.mkdtemp()
            sigmf_md_path = sigmffile.name + SIGMF_METADATA_EXT
            sigmf_md_path = os.path.join(tmpdir, sigmf_md_path)
            sigmf_data_path = sigmffile.name + SIGMF_DATASET_EXT
            sigmf_data_path = os.path.join(tmpdir, sigmf_data_path)

            with open(sigmf_md_path, "w") as mdfile:
                sigmffile.dump(mdfile, pretty=True)

            shutil.copy(sigmffile.data_file, sigmf_data_path)
            sigmf_archive.add(tmpdir, arcname=sigmffile.name, filter=chmod)
            shutil.rmtree(tmpdir)

        sigmf_archive.close()
        if fileobj is None:
            sigmf_fileobj.close()
        else:
            sigmf_fileobj.seek(0)  # ensure next open can read this as a tar

        self.path = sigmf_archive.name

    def _check_input(self):
        self._ensure_path_has_correct_extension()
        for sf in self.sigmffiles:
            self._ensure_sigmffile_name_set(sf)
            self._ensure_sigmffile_data_file_set(sf)
            self._validate_sigmffile_metadata(sf)

    def _ensure_path_has_correct_extension(self):
        path = self.path
        if path is None:
            return

        has_extension = "." in path
        has_correct_extension = path.endswith(SIGMF_ARCHIVE_EXT)
        if has_extension and not has_correct_extension:
            apparent_ext = os.path.splitext(path)[-1]
            err = "extension {} != {}".format(apparent_ext, SIGMF_ARCHIVE_EXT)
            raise error.SigMFFileError(err)

        self.path = path if has_correct_extension else path + SIGMF_ARCHIVE_EXT

    @staticmethod
    def _ensure_sigmffile_name_set(sf):
        if not sf.name:
            err = "the `name` attribute must be set to pass to `SigMFArchive`"
            raise error.SigMFFileError(err)

    @staticmethod
    def _ensure_sigmffile_data_file_set(sf):
        if not sf.data_file:
            err = "no data file - use `set_data_file`"
            raise error.SigMFFileError(err)

    @staticmethod
    def _validate_sigmffile_metadata(sf):
        valid_md = sf.validate()
        if not valid_md:
            err = "invalid metadata - {!s}"
            raise error.SigMFValidationError(err.format(valid_md))

    def _get_output_fileobj(self):
        try:
            fileobj = self._get_open_fileobj()
        except:
            if self.fileobj:
                e = "fileobj {!r} is not byte-writable".format(self.fileobj)
            else:
                e = "can't open {!r} for writing".format(self.path)

            raise error.SigMFFileError(e)

        return fileobj

    def _get_open_fileobj(self):
        if self.fileobj:
            fileobj = self.fileobj
            fileobj.write(bytes())  # force exception if not byte-writable
        else:
            fileobj = open(self.path, "wb")

        return fileobj


def extract(archive_path, dir=None):
    """Extract an archive and return a list of `SigMFFile`s.

    If `dir` is given, extract the archive to that directory. Otherwise,
    the archive will be extracted to a temporary directory. For example,
    `dir` == "." will extract the archive into the current working
    directory.

    """
    # Need to import here to avoid circular imports
    from .sigmffile import SigMFFile

    if not dir:
        dir = tempfile.mkdtemp()

    archive = tarfile.open(archive_path, mode="r", format=tarfile.PAX_FORMAT)
    members = archive.getmembers()

    sigmffiles = []

    try:
        archive.extractall(path=dir)

        data_file = None
        metadata = None

        for member in members:
            if member.name.endswith(SIGMF_DATASET_EXT):
                data_file = os.path.join(dir, member.name)
            elif member.name.endswith(SIGMF_METADATA_EXT):
                bytestream_reader = codecs.getreader("utf-8")  # bytes -> str
                mdfile_reader = bytestream_reader(archive.extractfile(member))
                metadata = json.load(mdfile_reader)

            if data_file is not None and metadata is not None:
                sf = SigMFFile(metadata=metadata, data_file=data_file)
                sigmffiles.append(sf)
                data_file = None
                metadata = None
    finally:
        archive.close()

    return sigmffiles
