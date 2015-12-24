from __future__ import absolute_import, unicode_literals, print_function

import os
import time
import zlib

import fcntl
import marshal
from contextlib import contextmanager

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging

from gevent import queue
from gevent.lock import RLock


@contextmanager
def flock(fd):
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


class FileQueue(queue.Queue):
    persistent = True

    STOPPED = False

    shm_size = len(marshal.dumps((0, 0, 0)))
    bucket_size = 10 * 1024 * 1024  # 10MB
    sync_age = 500

    def __init__(self, name=None, log=logging):
        self.name = name
        self.log = log

        self.lock = RLock()
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

        self.seq = 0

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
            except Exception:
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

    def _peek_file(self):
        try:
            seq = -1
            tries = -1
            while seq < self.seq:
                tries += 1
                seq, crc32, value = marshal.load(self.fread)
                if crc32 != zlib.crc32(value):
                    raise ValueError
            return seq, value
        except EOFError:
            if tries > 10:
                return False
            return None  # The file could not be read, ignore
        except (ValueError, TypeError):
            # New, or perhaps empty/corrupt pos file (since the position offset has an invalid marshal)
            return False

    def __del__(self):
        self.fpos.close()
        if self.fwrite:
            self.fwrite.close()
        if self.fread:
            self.fread.close()

    def get(self, block=True, timeout=None):
        start = time.time()
        timeout = (timeout if block else 0) or 0
        value = fvalue = None

        try:
            qseq, _ = self.peek(block=False)
        except queue.Empty:
            qseq = self.seq

        if not self.lock.acquire(True, 5):
            raise RuntimeError("Could not lock queue")
        try:
            try:
                # Get nest file/offset (from shared memory):
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
            fvalue = self._peek_file()
            if fvalue:
                seq, fvalue = fvalue
                if seq < qseq:
                    # If the seq from the read value from the file is lower
                    # than the one in the memory queue, use the value.
                    self.seq = seq
                else:
                    fvalue = None
                sync_age += 1
                offset = self.fread.tell()
                if offset > self.bucket_size:
                    self._reading(frnum + 1)
                    self._cleanup(frnum)
                    sync_age = self.sync_age  # Force updateing position
                    offset = 0
            elif fvalue is False:
                # Early update position
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

        _timeout = timeout - (time.time() - start)
        if _timeout < 0:
            _timeout = 0
        _block = bool(_timeout)

        try:
            seq, value = super(FileQueue, self).get(block=_block, timeout=_timeout)
            self.seq = seq
        except queue.Empty:
            if not _block:
                raise

        if fvalue:
            value = pickle.loads(zlib.decompress(fvalue))
        return value

    def put(self, item, block=True, timeout=None):
        seq = time.time()

        super(FileQueue, self).put((seq, item), block=block, timeout=timeout)

        _item = zlib.compress(pickle.dumps(item))
        crc32 = zlib.crc32(_item)
        with flock(self.fwrite) as fwrite:
            marshal.dump((seq, crc32, _item), fwrite)
            fwrite.flush()
            os.fsync(fwrite.fileno())
            offset = fwrite.tell()
        if offset > self.bucket_size:
            # Switch to a new queue file:
            self._writing(self.fwnum + 1)
