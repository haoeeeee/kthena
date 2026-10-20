# Copyright The Volcano Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import fcntl
import os
from typing import Optional, IO

from kthena.downloader.logger import setup_logger

logger = setup_logger()


class LockError(Exception):
    pass


class LockManager:
    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._lock_file: Optional[IO] = None
        self._is_locked = False

    def __enter__(self) -> 'LockManager':
        if not self.try_acquire():
            raise LockError(f"Failed to acquire lock: {self.lock_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    @property
    def is_locked(self) -> bool:
        return self._is_locked

    def try_acquire(self) -> bool:
        if self._is_locked:
            return True
        try:
            lock_dir = os.path.dirname(self.lock_path)
            os.makedirs(lock_dir, exist_ok=True)
            try:
                fd = os.open(
                    self.lock_path,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL,
                    0o666,
                )
                created = True
            except FileExistsError:
                fd = os.open(self.lock_path, os.O_RDWR)
                created = False
            try:
                # The lock contains no secrets, and the cache directory is already
                # shared writable storage. Ignore the process umask so pods running
                # as different UIDs can contend on the same persistent inode.
                if created:
                    os.fchmod(fd, 0o666)
                self._lock_file = os.fdopen(fd, "r+")
            except Exception:
                os.close(fd)
                raise
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._is_locked = True
            logger.info(f"Lock acquired: {self.lock_path}")
            return True
        except BlockingIOError:
            self._cleanup()
            return False
        except Exception as e:
            logger.error(f"Error acquiring lock: {e}")
            self._cleanup()
            return False

    def release(self) -> None:
        if not self._is_locked:
            return
        self._cleanup()
        logger.info(f"Lock released: {self.lock_path}")

    def _cleanup(self) -> None:
        if self._lock_file:
            try:
                self._lock_file.close()
            except Exception as e:
                logger.error(f"Error while closing lock file: {e}")
        self._lock_file = None
        self._is_locked = False
