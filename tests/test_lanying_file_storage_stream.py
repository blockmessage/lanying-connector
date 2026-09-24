import importlib.util
import sys
import types
import unittest
from unittest import mock
from pathlib import Path


def load_file_storage_module():
    path = Path(__file__).resolve().parents[1] / 'lanying_file_storage.py'
    spec = importlib.util.spec_from_file_location(
        'lanying_file_storage_stream_test_target', path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {
            'lanying_url_loader': types.ModuleType('lanying_url_loader'),
            'lanying_utils': types.ModuleType('lanying_utils')}):
        spec.loader.exec_module(module)
    return module


lanying_file_storage = load_file_storage_module()


class FakeResponse:
    def __init__(self, content):
        self.content = content
        self.offset = 0
        self.close_count = 0
        self.release_count = 0

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self.content) - self.offset
        start = self.offset
        self.offset = min(len(self.content), self.offset + size)
        return self.content[start:self.offset]

    def close(self):
        self.close_count += 1

    def release_conn(self):
        self.release_count += 1


class FileStorageStreamTests(unittest.TestCase):
    def test_open_object_streams_and_releases_connection_once(self):
        response = FakeResponse(b'file-content')
        fake_client = mock.Mock()
        fake_client.stat_object.return_value = mock.Mock(size=len(response.content))
        fake_client.get_object.return_value = response

        with mock.patch.object(lanying_file_storage, 'client', fake_client):
            result = lanying_file_storage.open_object('embedding/app/doc.pdf')

        self.assertEqual('ok', result['result'])
        self.assertEqual(len(response.content), result['content_length'])
        self.assertEqual(b'file-', result['stream'].read(5))
        self.assertEqual(b'content', result['stream'].read())
        result['stream'].close()
        result['stream'].close()
        self.assertEqual(1, response.close_count)
        self.assertEqual(1, response.release_count)

    def test_open_object_rejects_file_over_limit_before_download(self):
        fake_client = mock.Mock()
        fake_client.stat_object.return_value = mock.Mock(
            size=lanying_file_storage.max_upload_file_size + 1)

        with mock.patch.object(lanying_file_storage, 'client', fake_client):
            result = lanying_file_storage.open_object('embedding/app/large.pdf')

        self.assertEqual('error', result['result'])
        fake_client.get_object.assert_not_called()


if __name__ == '__main__':
    unittest.main()
