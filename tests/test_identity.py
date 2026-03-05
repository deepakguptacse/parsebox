"""Tests for parsebox user identity."""

from pathlib import Path

from parsebox.identity import get_or_create_user_id


class TestGetOrCreateUserId:
    def test_creates_new_id(self, tmp_path):
        id_file = tmp_path / ".user_id"
        import parsebox.identity as mod
        original = mod._USER_ID_FILE
        mod._USER_ID_FILE = id_file
        try:
            user_id = get_or_create_user_id()
            assert user_id
            assert len(user_id) == 36  # uuid4 string
            assert id_file.read_text().strip() == user_id
        finally:
            mod._USER_ID_FILE = original

    def test_returns_existing_id(self, tmp_path):
        id_file = tmp_path / ".user_id"
        id_file.write_text("existing-uuid-1234")
        import parsebox.identity as mod
        original = mod._USER_ID_FILE
        mod._USER_ID_FILE = id_file
        try:
            user_id = get_or_create_user_id()
            assert user_id == "existing-uuid-1234"
        finally:
            mod._USER_ID_FILE = original

    def test_idempotent(self, tmp_path):
        id_file = tmp_path / ".user_id"
        import parsebox.identity as mod
        original = mod._USER_ID_FILE
        mod._USER_ID_FILE = id_file
        try:
            first = get_or_create_user_id()
            second = get_or_create_user_id()
            assert first == second
        finally:
            mod._USER_ID_FILE = original
