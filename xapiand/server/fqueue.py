from __future__ import absolute_import, unicode_literals, print_function

import os
import time
import zlib

import fcntl
import marshal
import multiprocessing
from contextlib import contextmanager

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging


@contextmanager
def flock(fd):
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


class FileQueue(object):
    STOPPED = False

    shm_size = len(marshal.dumps((0, 0, 0)))
    bucket_size = 10 * 1024 * 1024  # 10MB
    sync_age = 500

    def __init__(self, name=None, log=logging):
        self.name = name
        self.log = log

        self.sem = multiprocessing.Semaphore()
        self.lock = multiprocessing.RLock()
        self.spos = None

        fnamepos = '%s.pos' % self.name
        if not os.path.exists(fnamepos):
            with open(fnamepos, 'wb') as f:  # touch file
                marshal.dump((0, 0, 0), f)

        self.fpos = open(fnamepos, 'r+b')

        self.fread = None
        self.frnum = None

        self.fwrite = None
        self.fwnum = None

        frnum, _ = self._update_pos()
        self._writing(frnum)

    def _update_pos(self, fnum=None, offset=None):
        with flock(self.fpos) as fpos:
            fpos.seek(0)
            try:
                _fnum, _offset = marshal.load(fpos)
            except (EOFError, ValueError, TypeError):
                _fnum, _offset = 0, 0  # New, perhaps empty or corrupt pos file
            finally:
                if fnum is not None and offset is not None:
                    fpos.seek(0)
                    marshal.dump((fnum, offset), fpos)
                    fpos.flush()
                    os.fsync(fpos.fileno())
        return _fnum, _offset

    def _cleanup(self, fnum):
        """
        Deletes all files for the queue up to, and including, fnum.

        """
        while os.path.exists('%s.%s' % (self.name, fnum)):
            try:
                fname = '%s.%s' % (self.name, fnum)
                os.unlink(fname)
                # self.log.debug("Cleaned up file: %s", fname)
            except:
                pass
            fnum -= 1

    def _reading(self, frnum):
        if self.frnum == frnum:
            return
        if self.fread:
            self.fread.close()
        fname = '%s.%s' % (self.name, frnum)
        if not os.path.exists(fname):
            open(fname, 'wb').close()  # touch file
        self.fread = open(fname, 'rb')
        self.frnum = frnum
        # self.log.debug("New read bucket: %s", self.frnum)

    def _writing(self, fwnum):
        _fwnum = fwnum
        while os.path.exists('%s.%s' % (self.name, _fwnum)):
            fwnum = _fwnum
            _fwnum += 1
        if self.fwnum == fwnum:
            return
        if self.fwrite:
            self.fwrite.close()
        self.fwrite = open('%s.%s' % (self.name, fwnum), 'ab')
        self.fwnum = fwnum
        # self.log.debug("New write bucket: %s", self.fwnum)

    def __del__(self):
        self.fpos.close()
        if self.fwrite:
            self.fwrite.close()
        if self.fread:
            self.fread.close()

    def peek(self):
        try:
            crc32, value = marshal.load(self.fread)
            if crc32 != zlib.crc32(value):
                raise ValueError
            return value
        except EOFError:
            return None  # The file could not be read, ignore
        except (ValueError, TypeError):
            # New, or perhaps empty/corrupt pos file (since the position offset has an invalid marshal)
            return False

    def get(self, block=True, timeout=None):
        start = time.time()
        value = None
        timeout = timeout or 0 if block else 0
        while not value and not self.STOPPED:
            # Try acquiring the semaphore (in case there's something to read)
            self.sem.acquire(block, timeout)
            if not self.lock.acquire(True, 5):
                raise RuntimeError("Could not lock queue")
            try:
                # Get nest file/offset (from shared memory):
                try:
                    frnum, offset, sync_age, sync_time = self.spos
                except (ValueError, TypeError):
                    # New, perhaps empty or corrupt pos file
                    frnum, offset = self._update_pos()
                    sync_time = start
                    sync_age = 0
                # self.log.debug('%s @ %s' % (self.name, repr((frnum, offset, sync_age))))

                # Open proper queue file for reading (if it isn't open yet):
                self._reading(frnum)
                self.fread.seek(offset)

                # Read from the queue
                value = self.peek()
                if value:
                    sync_age += 1
                    offset = self.fread.tell()
                    if offset > self.bucket_size:
                        self._reading(frnum + 1)
                        self._cleanup(frnum)
                        sync_age = self.sync_age  # Force updateing position
                        offset = 0
                elif value is False:
                    frnum, offset = self._update_pos()
                    sync_time = start
                    sync_age = 0

                # Update position
                if sync_age >= self.sync_age or sync_age and start - sync_time > 10 or self.STOPPED:
                    self._update_pos(self.frnum, offset)
                    sync_time = start
                    sync_age = 0
                self.spos = (self.frnum, offset, sync_age, sync_time)

            finally:
                self.lock.release()

            if time.time() - start > timeout:
                break

        if value:
            if self.peek():
                # If there's something more to read in the file,
                # release (or re-release) the semaphore.
                self.sem.release()
            return pickle.loads(zlib.decompress(value))

    def put(self, value, block=True, timeout=None):
        value = zlib.compress(pickle.dumps(value))
        crc32 = zlib.crc32(value)
        with flock(self.fwrite) as fwrite:
            marshal.dump((crc32, value), fwrite)
            fwrite.flush()
            os.fsync(fwrite.fileno())
            offset = fwrite.tell()
        self.sem.release()
        if offset > self.bucket_size:
            # Switch to a new queue file:
            self._writing(self.fwnum + 1)
