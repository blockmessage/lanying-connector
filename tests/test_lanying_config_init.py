import unittest
from unittest import mock

import lanying_config


class _Meta:
    key = b'/apps/app-1.lanying_connector'


class _EtcdClient:
    def __init__(self, fail=False):
        self.fail = fail
        self.watch = None

    def get_prefix(self, prefix):
        if self.fail:
            raise RuntimeError('etcd unavailable')
        return [(b'{"product_id":7002}', _Meta())]

    def add_watch_prefix_callback(self, prefix, callback):
        self.watch = (prefix, callback)


class LanyingConfigInitTests(unittest.TestCase):
    def setUp(self):
        self.old_mode = lanying_config.mode
        self.old_etcd = lanying_config.etcd
        self.old_prefix = lanying_config.prefix
        self.old_configs = dict(lanying_config.configs)
        lanying_config.mode = 'env'
        lanying_config.etcd = None
        lanying_config.prefix = '/apps/'
        lanying_config.configs.clear()

    def tearDown(self):
        lanying_config.mode = self.old_mode
        lanying_config.etcd = self.old_etcd
        lanying_config.prefix = self.old_prefix
        lanying_config.configs.clear()
        lanying_config.configs.update(self.old_configs)

    def test_init_loads_config_and_registers_watch(self):
        client = _EtcdClient()
        with mock.patch.dict('os.environ', {
                'LANYING_CONNECTOR_ETCD_SERVER': 'etcd',
                'LANYING_CONNECTOR_ETCD_PORT': '2379'}, clear=False), mock.patch.object(
                lanying_config.etcd3, 'client', return_value=client):
            lanying_config.init()

        self.assertIs(client, lanying_config.etcd)
        self.assertEqual('etcd', lanying_config.mode)
        self.assertEqual(7002, lanying_config.configs[
            '/apps/app-1.lanying_connector']['product_id'])
        self.assertEqual('/apps/', client.watch[0])

    def test_failed_init_can_be_retried(self):
        failing_client = _EtcdClient(fail=True)
        working_client = _EtcdClient()
        with mock.patch.dict('os.environ', {
                'LANYING_CONNECTOR_ETCD_SERVER': 'etcd',
                'LANYING_CONNECTOR_ETCD_PORT': '2379'}, clear=False), mock.patch.object(
                lanying_config.etcd3, 'client',
                side_effect=[failing_client, working_client]):
            with self.assertRaises(RuntimeError):
                lanying_config.init()
            self.assertIsNone(lanying_config.etcd)
            lanying_config.init()

        self.assertIs(working_client, lanying_config.etcd)


if __name__ == '__main__':
    unittest.main()
