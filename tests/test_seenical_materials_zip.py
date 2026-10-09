"""Real ZIP parsing with external model/storage calls mocked."""
import ast
import io
import json
import logging
import shutil
import stat
import tempfile
import types
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import lanying_seenical_materials as m


class MaterialArchiveTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.path = self.directory / 'source.zip'

    def write_zip(self, members, compression=zipfile.ZIP_DEFLATED):
        with zipfile.ZipFile(self.path, 'w', compression=compression) as archive:
            for name, content in members:
                archive.writestr(name, content)

    def extract(self):
        return m.extract_archive_documents(str(self.path), str(self.directory), ['.txt', '.md', '.pdf', '.docx'])

    def test_supported_members_share_archive_and_skip_non_documents(self):
        self.write_zip([('资料/说明.md', '产品说明'), ('notes.txt', 'Facts'),
                        ('pictures/a.png', b'image'), ('nested.zip', b'zip'),
                        ('__MACOSX/._notes.txt', b'metadata'), ('.DS_Store', b'metadata'),
                        ('empty.txt', b''), ('folder/', b'')])
        documents, size, skipped = self.extract()
        self.assertEqual([row[0] for row in documents], ['资料/说明.md', 'notes.txt'])
        self.assertEqual(size, len('产品说明'.encode()) + 5)
        self.assertEqual(skipped, 5)
        self.assertEqual(Path(documents[0][1]).read_text(), '产品说明')
        self.assertEqual(Path(documents[0][1]).parent, self.directory)

    def test_unsafe_member_paths_and_special_files_are_rejected(self):
        for name in ['../outside.txt', '/outside.txt', 'safe/../../outside.txt',
                     'C:\\outside.txt', '..\\outside.txt', 'file.txt:stream']:
            with self.subTest(name=name):
                self.write_zip([(name, b'text')])
                with self.assertRaisesRegex(m.MaterialError, 'material_archive_unsafe'):
                    self.extract()
        for mode in [stat.S_IFLNK, stat.S_IFIFO]:
            entry = zipfile.ZipInfo('special.txt')
            entry.create_system = 3
            entry.external_attr = (mode | 0o777) << 16
            self.write_zip([(entry, b'target')])
            with self.assertRaisesRegex(m.MaterialError, 'material_archive_unsafe'):
                self.extract()

    def test_legacy_chinese_filenames_and_paths(self):
        for encoding in ['gbk', 'utf-8']:
            with self.subTest(encoding=encoding):
                raw_name = '资料.txt'.encode(encoding)
                placeholder = b'x' * (len(raw_name) - 4) + b'.txt'
                self.write_zip([(placeholder.decode('ascii'), b'facts')])
                # Patch both ZIP headers, keeping lengths and the unset UTF-8 flag.
                self.path.write_bytes(self.path.read_bytes().replace(placeholder, raw_name))
                documents, size, skipped = self.extract()
                self.assertEqual(documents[0][0], '资料.txt')
                self.assertEqual(Path(documents[0][1]).read_bytes(), b'facts')
                self.assertEqual((size, skipped), (5, 0))
        self.write_zip([('../xxxx.txt', b'facts')])
        self.path.write_bytes(self.path.read_bytes().replace(b'xxxx.txt', '资料.txt'.encode('gbk')))
        with self.assertRaisesRegex(m.MaterialError, 'material_archive_unsafe'):
            self.extract()

    def test_utf8_flag_preserves_filename_without_legacy_redecoding(self):
        self.write_zip([('╫╩┴╧.txt', b'facts')])
        self.assertEqual(self.extract()[0][0][0], '╫╩┴╧.txt')

    def test_empty_or_unsupported_only_archive_is_not_ready(self):
        for members in [[], [('a.png', b'image')], [('empty.txt', b'')], [('nested.zip', b'zip')]]:
            self.write_zip(members)
            with self.assertRaisesRegex(m.MaterialError, 'material_archive_empty'):
                self.extract()

    def test_archive_entry_total_and_individual_limits(self):
        self.write_zip([('a.txt', b'123'), ('b.md', b'456')])
        for field, value, error in [('MAX_ARCHIVE_ENTRIES', 1, 'material_archive_limit'),
                                    ('MAX_ARCHIVE_BYTES', 5, 'material_archive_limit'),
                                    ('MAX_FILE_BYTES', 2, 'material_file_too_large')]:
            with mock.patch.object(m, field, value):
                with self.assertRaisesRegex(m.MaterialError, error):
                    self.extract()
        # Ignored files still count towards archive safety limits.
        self.write_zip([('a.txt', b'12'), ('a.png', b'3456')])
        with mock.patch.object(m, 'MAX_ARCHIVE_BYTES', 5):
            with self.assertRaisesRegex(m.MaterialError, 'material_archive_limit'):
                self.extract()

    def test_actual_read_size_is_bounded_not_only_metadata(self):
        self.write_zip([('a.txt', b'1')])
        with mock.patch.object(m, 'MAX_FILE_BYTES', 10), \
                mock.patch.object(zipfile.ZipFile, 'open', return_value=io.BytesIO(b'x' * 11)):
            with self.assertRaisesRegex(m.MaterialError, 'material_archive_limit'):
                self.extract()

    def test_encryption_and_nonstandard_compression_are_rejected(self):
        self.write_zip([('a.txt', b'1')])
        original = zipfile.ZipFile.infolist
        def encrypted(archive):
            entries = original(archive)
            entries[0].flag_bits |= 1
            return entries
        with mock.patch.object(zipfile.ZipFile, 'infolist', encrypted):
            with self.assertRaisesRegex(m.MaterialError, 'material_archive_unsupported'):
                self.extract()
        self.write_zip([('a.txt', b'1')], zipfile.ZIP_BZIP2)
        with self.assertRaisesRegex(m.MaterialError, 'material_archive_unsupported'):
            self.extract()

    def test_corrupt_zip_and_crc_and_invalid_document_fail(self):
        self.path.write_bytes(b'not a zip')
        with self.assertRaisesRegex(m.MaterialError, 'material_archive_invalid'):
            self.extract()
        self.write_zip([('a.txt', b'facts')], zipfile.ZIP_STORED)
        data = bytearray(self.path.read_bytes())
        data[30 + len('a.txt')] ^= 1
        self.path.write_bytes(data)
        with self.assertRaisesRegex(m.MaterialError, 'material_archive_invalid'):
            self.extract()
        self.write_zip([('a.txt', b'good'), ('broken.pdf', b'not a PDF')])
        with self.assertRaisesRegex(m.MaterialError, 'material_unsupported_format'):
            self.extract()

    def indexing_environment(self):
        meta = {'object_name': 'original.zip', 'status': 'finish'}
        embedding = types.SimpleNamespace(
            allow_exts=lambda: ['.txt', '.md', '.pdf'],
            get_doc=mock.Mock(return_value=meta), create_doc_info=mock.Mock(),
            update_doc_field=mock.Mock(side_effect=lambda u, d, field, value: meta.update({field: value})),
            process_embedding_file=mock.Mock())
        files = types.SimpleNamespace(download=mock.Mock(side_effect=lambda obj, target:
            (shutil.copyfile(self.path, target), {'result': 'ok'})[1]))
        config = types.SimpleNamespace(get_lanying_connector=lambda app: {'product_id': '1'},
                                       get_lanying_connector_deduct_failed=lambda app: False)
        return embedding, files, config, meta

    def test_index_uses_one_doc_id_and_uncompressed_quota_and_preserves_original(self):
        self.write_zip([('docs/a.txt', b'a' * 10000), ('b.md', b'b' * 20000), ('image.png', b'ignored')])
        embedding, files, config, meta = self.indexing_environment()
        meta.update(progress_total=9, progress_finish=9)
        modules = {'lanying_embedding': embedding, 'lanying_file_storage': files, 'lanying_config': config}
        doc = {'doc_id': '1-zip', 'filename': '资料.zip', 'object_name': 'original.zip'}
        with mock.patch.dict('sys.modules', modules), mock.patch.object(m, 'initialize_index'), \
                mock.patch.object(m, 'update_usage') as usage:
            m._index_document('a', {'embedding_uuid': '1'}, doc)
        usage.assert_called_once_with('a', '1', '1-zip', 30000)
        calls = embedding.process_embedding_file.call_args_list
        self.assertEqual([call.args[5] for call in calls], ['1-zip', '1-zip'])
        self.assertEqual([call.kwargs['source_filename'] for call in calls], ['docs/a.txt', 'b.md'])
        self.assertEqual(meta['archive_document_count'], 2)
        self.assertEqual(meta['archive_skipped_count'], 1)
        self.assertEqual(meta['archive_indexed_size'], 30000)
        self.assertEqual(meta['progress_total'], 0)
        self.assertEqual(meta['progress_finish'], 0)
        self.assertEqual(doc['object_name'], 'original.zip')
        self.assertLess(self.path.stat().st_size, 30000)

    def test_invalid_member_rejected_before_quota_or_any_model_call(self):
        self.write_zip([('a.txt', b'good'), ('bad.pdf', b'bad')])
        embedding, files, config, meta = self.indexing_environment()
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding, 'lanying_file_storage': files,
                                            'lanying_config': config}), \
                mock.patch.object(m, 'initialize_index'), mock.patch.object(m, 'update_usage') as usage:
            with self.assertRaises(m.MaterialError):
                m._index_document('a', {'embedding_uuid': '1'},
                                  {'doc_id': '1-zip', 'filename': 'a.zip', 'object_name': 'original.zip'})
        usage.assert_not_called()
        embedding.process_embedding_file.assert_not_called()

    def test_quota_failure_does_not_start_indexing(self):
        self.write_zip([('a.txt', b'good')])
        embedding, files, config, meta = self.indexing_environment()
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding, 'lanying_file_storage': files,
                                            'lanying_config': config}), \
                mock.patch.object(m, 'initialize_index'), \
                mock.patch.object(m, 'update_usage', side_effect=m.MaterialError('material_storage_limit')):
            with self.assertRaisesRegex(m.MaterialError, 'material_storage_limit'):
                m._index_document('a', {'embedding_uuid': '1'},
                                  {'doc_id': '1-zip', 'filename': 'a.zip', 'object_name': 'original.zip'})
        embedding.process_embedding_file.assert_not_called()

    def test_member_parser_failure_stops_remaining_archive_documents(self):
        self.write_zip([('a.txt', b'first'), ('b.md', b'second'), ('c.txt', b'third')])
        embedding, files, config, meta = self.indexing_environment()
        embedding.process_embedding_file.side_effect = [None, ValueError('parser failed')]
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding, 'lanying_file_storage': files,
                                            'lanying_config': config}), \
                mock.patch.object(m, 'initialize_index'), mock.patch.object(m, 'update_usage') as usage:
            with self.assertRaisesRegex(ValueError, 'parser failed'):
                m._index_document('a', {'embedding_uuid': '1'},
                                  {'doc_id': '1-zip', 'filename': 'a.zip', 'object_name': 'original.zip'})
        self.assertEqual(embedding.process_embedding_file.call_count, 2)
        usage.assert_called_once_with('a', '1', '1-zip', 16)

    def test_source_filename_does_not_change_shared_library_config_or_legacy_calls(self):
        source = Path(__file__).resolve().parents[1] / 'lanying_embedding.py'
        nodes = [n for n in ast.parse(source.read_text()).body
                 if isinstance(n, ast.FunctionDef) and n.name == 'process_embedding_file']
        config = {'vendor': 'openai'}
        processor = mock.Mock()
        namespace = {'lanying_redis': types.SimpleNamespace(get_redis_stack_connection=lambda: mock.Mock()),
                     'increase_embedding_doc_field': mock.Mock(), 'update_doc_field': mock.Mock(),
                     'get_doc': lambda *args: {'progress_total': '3'},
                     'get_embedding_uuid_info': lambda u: config, 'process_txt': processor,
                     'logging': logging}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), namespace)
        namespace['process_embedding_file']('', 'a', '1', 'path', 'name', 'doc', '.txt', source_filename='docs/a.txt')
        self.assertEqual(processor.call_args.args[0]['_seenical_source_filename'], 'docs/a.txt')
        self.assertEqual(processor.call_args.args[0]['_seenical_progress_offset'], 3)
        self.assertNotIn('_seenical_source_filename', config)
        self.assertNotIn('_seenical_progress_offset', config)
        namespace['process_embedding_file']('', 'a', '1', 'path', 'name', 'doc', '.txt')
        self.assertIs(processor.call_args.args[0], config)

    def test_archive_progress_accumulates_members_and_incremental_parser_totals(self):
        source = Path(__file__).resolve().parents[1] / 'lanying_embedding.py'
        names = {'process_embedding_file', 'update_progress', 'update_progress_total'}
        nodes = [n for n in ast.parse(source.read_text()).body
                 if isinstance(n, ast.FunctionDef) and n.name in names]
        meta = {'progress_total': 0, 'progress_finish': 0}
        redis = mock.Mock()
        redis.hset.side_effect = lambda key, field, value: meta.update({field: value})
        redis.hincrby.side_effect = lambda key, field, value: meta.update({field: meta.get(field, 0) + value})
        namespace = {'lanying_redis': types.SimpleNamespace(get_redis_stack_connection=lambda: redis),
                     'increase_embedding_doc_field': mock.Mock(), 'update_doc_field': mock.Mock(),
                     'get_embedding_uuid_info': lambda *args: {}, 'get_doc': lambda *args: meta,
                     'logging': logging}
        config = {'vendor': 'openai'}
        namespace['get_embedding_uuid_info'] = lambda *args: config
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), namespace)
        def process(config, *args):
            # Spreadsheet/PPT parsers may update the same member's total repeatedly.
            namespace['update_progress_total'](redis, 'doc', 1, config)
            namespace['update_progress'](redis, 'doc', 1)
            namespace['update_progress_total'](redis, 'doc', 3, config)
            namespace['update_progress'](redis, 'doc', 2)
        namespace['process_txt'] = process
        for name in ['a.txt', 'b.txt']:
            namespace['process_embedding_file']('', 'a', '1', 'path', name, 'doc', '.txt', source_filename=name)
        self.assertEqual(meta, {'progress_total': 6, 'progress_finish': 6})
        self.assertNotIn('_seenical_progress_offset', config)
        # Existing standalone processing still replaces totals instead of adding.
        namespace['update_progress_total'](redis, 'doc', 2, config)
        self.assertEqual(meta['progress_total'], 2)
        namespace['update_progress_total'](redis, 'doc', 1)
        self.assertEqual(meta['progress_total'], 1)

    def test_indexed_chunks_include_source_filename_only_for_archive_members(self):
        source = Path(__file__).resolve().parents[1] / 'lanying_embedding.py'
        nodes = [n for n in ast.parse(source.read_text()).body
                 if isinstance(n, ast.FunctionDef) and n.name == 'insert_embeddings']
        redis = mock.Mock()
        namespace = {'json': json, 'logging': logging,
                     'lanying_vendor': types.SimpleNamespace(get_embedding_model_config=lambda *args: {'model': 'ada'}),
                     'get_max_token_count': lambda config: 1000, 'num_of_tokens': len,
                     'get_doc': lambda *args: {'tags': {}}, 'generate_block_id': lambda *args: 'block',
                     'maybe_rate_limit': mock.Mock(), 'fetch_embedding': mock.Mock(return_value=[0.1]),
                     'get_embedding_data_key': lambda *args: 'key', 'sha256': lambda value: value,
                     'np': types.SimpleNamespace(array=lambda value: types.SimpleNamespace(tobytes=lambda: b'vector')),
                     'text_byte_size': lambda value: len(value.encode()),
                     'increase_embedding_uuid_field': mock.Mock(), 'increase_embedding_doc_field': mock.Mock(),
                     'update_doc_field': mock.Mock(), 'update_progress': mock.Mock(),
                     'get_embedding_doc_info_key': lambda *args: 'doc-key'}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), namespace)
        config = {'tags': [], '_seenical_source_filename': '资料/a.txt'}
        namespace['insert_embeddings'](config, 'a', '1', 'a.txt', 'doc', [(4, 'fact', {})], redis)
        stored = redis.hmset.call_args.args[1]
        self.assertEqual(stored['text'], '[来源文件："资料/a.txt"]\nfact')
        self.assertEqual(stored['num_of_tokens'], len(stored['text']))
        namespace['fetch_embedding'].assert_called_once_with('a', 'openai', {'model': 'ada'}, stored['text'], False)
        namespace['insert_embeddings']({'tags': []}, 'a', '1', 'a.txt', 'doc', [(4, 'fact', {})], redis)
        self.assertEqual(redis.hmset.call_args.args[1]['text'], 'fact')


if __name__ == '__main__':
    unittest.main()
