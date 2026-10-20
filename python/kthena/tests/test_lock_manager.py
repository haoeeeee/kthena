# Copyright The Volcano Authors
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

import os
import unittest
from unittest.mock import Mock, patch

from kthena.downloader.lock import LockManager, LockError
from kthena.tests.test_utils import create_temp_dir, cleanup_temp_dir


class TestLockManager(unittest.TestCase):
    def setUp(self):
        self.temp_dir = create_temp_dir()
        self.lock_path = os.path.join(self.temp_dir, "test.lock")
        self.lock_manager = LockManager(self.lock_path)

    def tearDown(self):
        if hasattr(self, "lock_manager") and self.lock_manager.is_locked:
            self.lock_manager.release()
        cleanup_temp_dir(self.temp_dir)

    def _acquire_lock(self, manager=None):
        manager = manager or self.lock_manager
        result = manager.try_acquire()
        self.assertTrue(result, "Failed to acquire lock")
        return result

    def _assert_lock_file_exists(self):
        self.assertTrue(os.path.exists(self.lock_path), "Lock file should exist")

    def _assert_lock_file_persists(self):
        self.assertTrue(
            os.path.exists(self.lock_path),
            "Lock file must keep a stable inode after release",
        )

    def test_lock_manager_acquire_release(self):
        self._acquire_lock()
        self._assert_lock_file_exists()
        self.lock_manager.release()
        self._assert_lock_file_persists()

    def test_lock_manager_context_manager(self):
        with LockManager(self.lock_path) as lock_manager:
            self._assert_lock_file_exists()
            self.assertTrue(lock_manager.is_locked, "Lock should be marked as locked")
        self._assert_lock_file_persists()

    def test_new_lock_file_is_made_shared_writable(self):
        with patch("os.fchmod", wraps=os.fchmod) as chmod:
            self._acquire_lock()
        chmod.assert_called_once()
        self.assertEqual(0o666, chmod.call_args.args[1])

    def test_existing_lock_file_does_not_require_chmod(self):
        self._acquire_lock()
        self.lock_manager.release()

        next_manager = LockManager(self.lock_path)
        with patch("os.fchmod", side_effect=PermissionError("not owner")) as chmod:
            self._acquire_lock(next_manager)
        chmod.assert_not_called()
        next_manager.release()

    def test_lock_file_inode_is_reused_after_release(self):
        self._acquire_lock()
        inode = os.stat(self.lock_path).st_ino

        self.lock_manager.release()

        next_manager = LockManager(self.lock_path)
        self._acquire_lock(next_manager)
        self.assertEqual(
            inode,
            os.stat(self.lock_path).st_ino,
            "Recreating the lock file permits independent locks on different inodes",
        )
        next_manager.release()

    @patch("kthena.downloader.lock.logger.error")
    def test_lock_manager_acquire_failure(self, mock_logger_error):
        lock_manager1 = LockManager(self.lock_path)
        lock_manager2 = LockManager(self.lock_path)

        self._acquire_lock(lock_manager1)
        self.assertFalse(
            lock_manager2.try_acquire(),
            "Second lock manager should fail to acquire the lock",
        )
        mock_logger_error.assert_not_called()
        lock_manager1.release()

    def test_lock_file_does_not_store_pid(self):
        self._acquire_lock()
        self.assertEqual(0, os.path.getsize(self.lock_path))

    def test_context_manager_failure(self):
        self._acquire_lock()
        with self.assertRaises(LockError):
            with LockManager(self.lock_path):
                pass
        self.lock_manager.release()

    def test_is_locked_property(self):
        self.assertFalse(
            self.lock_manager.is_locked, "is_locked should be False initially"
        )
        self._acquire_lock()
        self.assertTrue(
            self.lock_manager.is_locked, "is_locked should be True after acquisition"
        )
        self.lock_manager.release()
        self.assertFalse(
            self.lock_manager.is_locked, "is_locked should be False after release"
        )

    @patch("kthena.downloader.lock.logger.error")
    def test_lock_manager_acquire_exception_handling(self, mock_logger_error):
        with patch("os.makedirs", side_effect=OSError("Mocked error")):
            self.assertFalse(
                self.lock_manager.try_acquire(),
                "Acquire should fail due to mocked OSError",
            )
            mock_logger_error.assert_called_with("Error acquiring lock: Mocked error")

    @patch("kthena.downloader.lock.logger.error")
    def test_lock_manager_release_exception_handling(self, mock_logger_error):
        lock_file = Mock()
        lock_file.close.side_effect = OSError("Mocked error")
        self.lock_manager._lock_file = lock_file
        self.lock_manager._is_locked = True

        self.lock_manager.release()

        mock_logger_error.assert_called_with("Error while closing lock file: Mocked error")
        self.assertFalse(self.lock_manager.is_locked)

    def test_multiple_locks_management(self):
        lock_paths = [os.path.join(self.temp_dir, f"lock_{i}.lock") for i in range(3)]
        lock_managers = [LockManager(path) for path in lock_paths]

        for manager in lock_managers:
            self.assertTrue(manager.try_acquire(), "Failed to acquire multiple locks")

        for path in lock_paths:
            self.assertTrue(os.path.exists(path), "Lock file should exist")

        for manager in lock_managers:
            manager.release()

        for path in lock_paths:
            self.assertTrue(
                os.path.exists(path), "Lock file should keep a stable inode after release"
            )


if __name__ == "__main__":
    unittest.main()
