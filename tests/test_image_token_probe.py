import base64
from io import BytesIO
import unittest
from unittest import mock

from PIL import Image
import requests
import lanying_image as image


class ImageTokenProbeTests(unittest.TestCase):
    def setUp(self):
        buffer = BytesIO()
        with Image.new('RGB', (320, 240)) as picture:
            picture.save(buffer, format='PNG')
        self.png = buffer.getvalue()

    def response(self, chunks):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.iter_content.return_value = iter(chunks)
        return response

    def test_remote_probe_stops_as_soon_as_dimensions_are_available(self):
        def chunks():
            yield self.png
            raise AssertionError('should not read the rest of the image')
        response = self.response(chunks())
        with mock.patch.object(image.requests, 'get', return_value=response) as get:
            self.assertEqual((320, 240), image.get_image_size('https://example.test/image'))
        get.assert_called_once_with('https://example.test/image', stream=True, timeout=(3, 5))
        response.iter_content.assert_called_once_with(chunk_size=8192)
        response.raise_for_status.assert_called_once()
        response.__exit__.assert_called_once()

    def test_http_error_closes_response_and_keeps_existing_estimate(self):
        response = self.response([])
        response.raise_for_status.side_effect = requests.HTTPError('private-url')
        with mock.patch.object(image.requests, 'get', return_value=response), \
             self.assertLogs(level='WARNING') as logs:
            self.assertEqual((768, 2048), image.get_image_size('https://private-url'))
        response.iter_content.assert_not_called()
        response.__exit__.assert_called_once()
        self.assertNotIn('private-url', ' '.join(logs.output))

    def test_connection_and_body_timeouts_are_recoverable(self):
        with mock.patch.object(image.requests, 'get', side_effect=requests.Timeout):
            self.assertEqual((768, 2048), image.get_image_size('https://example.test/image'))
        response = self.response([])
        response.iter_content.side_effect = requests.Timeout
        with mock.patch.object(image.requests, 'get', return_value=response):
            self.assertEqual((768, 2048), image.get_image_size('https://example.test/image'))
        response.__exit__.assert_called_once()

    def test_probe_bounds_bytes_without_trusting_content_length(self):
        response = self.response([b'x' * 8, b'y' * 8, b'z' * 8])
        with mock.patch.object(image.requests, 'get', return_value=response), \
             mock.patch.object(image, 'IMAGE_PROBE_MAX_BYTES', 16):
            self.assertEqual((768, 2048), image.get_image_size('https://example.test/image'))
        response.__exit__.assert_called_once()

    def test_probe_checks_elapsed_time_while_reading(self):
        response = self.response([self.png])
        with mock.patch.object(image.requests, 'get', return_value=response), \
             mock.patch.object(image.time, 'monotonic', side_effect=[0, 11]):
            self.assertEqual((768, 2048), image.get_image_size('https://example.test/image'))
        response.__exit__.assert_called_once()

    def test_base64_probe_and_invalid_image(self):
        url = 'data:image/png;base64,' + base64.b64encode(self.png).decode('ascii')
        self.assertEqual((320, 240), image.get_image_size(url))
        with mock.patch.object(image.requests, 'get') as get:
            self.assertEqual((768, 2048), image.get_image_size('data:image/png;base64,bm90YW5pbWFnZQ=='))
        get.assert_not_called()

    def test_low_detail_does_not_fetch_image(self):
        with mock.patch.object(image, 'get_image_size_with_cache') as probe:
            self.assertEqual(85, image.calculate_tokens('https://example.test/image', 'low'))
        probe.assert_not_called()

    def test_cached_dimensions_do_not_download_again(self):
        redis = mock.Mock()
        with mock.patch.object(image.lanying_redis, 'get_redis_connection', return_value=redis), \
             mock.patch.object(image.lanying_redis, 'redis_get', return_value='320,240'), \
             mock.patch.object(image.requests, 'get') as get:
            self.assertEqual((320, 240), image.get_image_size_with_cache('https://example.test/image'))
        get.assert_not_called()


if __name__ == '__main__':
    unittest.main()
