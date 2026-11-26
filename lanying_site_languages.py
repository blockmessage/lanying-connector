"""Site language configuration and GitBook repository updates."""
import base64
import copy
import json
import posixpath
from pathlib import PurePosixPath
from urllib.parse import quote, urlsplit

import requests

LANGUAGES = {'zh-hans': '简体中文', 'en': 'English'}
COPY_FIELDS = ('title', 'meta_keywords', 'copyright', 'footer_note', 'hook_sentence_slogan',
               'hook_sentence_image', 'official_website_url', 'lanying_link')
URL_FIELDS = {'hook_sentence_image', 'official_website_url', 'lanying_link'}
TEMPLATE_API = 'https://api.github.com/repos/maxim-top/growai-gitbook'
TOOLING = {'.github', '.gitignore', '.bookignore', 'Makefile', 'book.json', 'scripts', 'tests', 'README-GrowAI.md'}


def normalize(value, primary):
    if not isinstance(value, dict) or set(value) != {'default_language', 'overrides'}:
        raise ValueError('Invalid site language settings.')
    overrides = value['overrides']
    if not isinstance(overrides, dict) or set(overrides) - (LANGUAGES.keys() - {primary}):
        raise ValueError('Invalid additional site language.')
    if value['default_language'] not in [primary, *overrides]:
        raise ValueError('Choose an enabled default language.')
    clean = {}
    for code, fields in overrides.items():
        if not isinstance(fields, dict) or set(fields) - set(COPY_FIELDS):
            raise ValueError('Invalid localized site fields.')
        clean[code] = {}
        for field in COPY_FIELDS:
            text = fields.get(field, '')
            if not isinstance(text, str) or len(text) > 20000:
                raise ValueError('Invalid localized site text.')
            if field in URL_FIELDS and text:
                url = urlsplit(text)
                if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password:
                    raise ValueError('Enter a valid website or image URL.')
            clean[code][field] = text
    return {'default_language': value['default_language'], 'overrides': clean}


def layout_for(primary, settings, legacy_root=''):
    return {
        'version': 2,
        'default_language': settings['default_language'],
        'legacy_root_language': legacy_root,
        'languages': {code: {'source_dir': code, 'public_path': '/' if code == legacy_root else f'/{code}/'}
                      for code in [primary, *settings['overrides']]}
    }


def release_language(layout, run, primary):
    if not layout or layout.get('version') != 2:
        return ''
    code = run.get('article_language') or primary
    if code not in LANGUAGES or code not in layout['languages']:
        raise ValueError('The article language is not enabled on the site. Add it in site settings.')
    return code


def scoped_directory(base, directory):
    parts = PurePosixPath(directory.lstrip('/')).parts
    if '..' in parts or '\\' in directory:
        raise ValueError('The publication folder must be inside its language directory.')
    return posixpath.join(base, *parts).rstrip('/') or '/'


class Repository:
    def __init__(self, api, headers):
        self.api = api
        self.headers = headers

    def request(self, method, path, *, missing_ok=False, **kwargs):
        response = requests.request(method, f'{self.api}/{path}', headers=self.headers, timeout=25, **kwargs)
        if missing_ok and response.status_code == 404:
            return None
        if not response.ok:
            raise ValueError(f'Could not update the site repository (HTTP {response.status_code}).')
        return response.json()

    def read(self, path, commit, optional=False):
        obj = self.request('GET', 'contents/' + quote(path, safe='/'),
                           params={'ref': commit}, missing_ok=optional)
        if isinstance(obj, list):
            raise ValueError(f'The directory already exists: {path}')
        return base64.b64decode(obj['content']) if obj is not None else None

    def json(self, path, commit, optional=False):
        data = self.read(path, commit, optional)
        return json.loads(data) if data is not None else None

    def upgrade_is_open(self, url):
        if not url:
            return False
        number = url.rsplit('/', 1)[-1]
        pull = self.request('GET', f'pulls/{number}', missing_ok=True)
        return bool(pull and pull['state'] == 'open')

    def open_upgrade(self, head, base):
        owner = self.api.rsplit('/', 2)[-2]
        pulls = self.request('GET', 'pulls', params={'state': 'open', 'head': f'{owner}:{head}', 'base': base})
        return pulls[0]['html_url'] if pulls else ''

    def commit(self, parent, entries, branch, upgrade_head):
        if upgrade_head and self.request('GET', f'git/ref/heads/{upgrade_head}', missing_ok=True):
            # GitHub accepted this branch on a previous attempt, but the PR may not exist yet.
            return self.open_upgrade(upgrade_head, branch) or self.create_upgrade_pr(upgrade_head, branch)
        if not entries:
            return ''
        base = self.request('GET', f'git/commits/{parent}')['tree']['sha']
        tree = self.request('POST', 'git/trees', json={'base_tree': base, 'tree': list(entries.values())})
        commit = self.request('POST', 'git/commits', json={
            'message': 'Configure Seenical site languages', 'parents': [parent], 'tree': tree['sha'],
        })['sha']
        if upgrade_head:
            self.request('POST', 'git/refs', json={'ref': f'refs/heads/{upgrade_head}', 'sha': commit})
            return self.create_upgrade_pr(upgrade_head, branch)
        self.request('PATCH', f'git/refs/heads/{branch}', json={'sha': commit, 'force': False})
        return ''

    def create_upgrade_pr(self, head, base):
        return self.request('POST', 'pulls', json={
            'head': head, 'base': base, 'title': 'Enable Seenical site languages',
            'body': 'Check the GitBook language preview from the build before merging. '
                    'The published site updates after the production build succeeds.',
        })['html_url']

    def blob(self, data):
        return self.request('POST', 'git/blobs', json={
            'content': base64.b64encode(data).decode('ascii'), 'encoding': 'base64',
        })['sha']


def entry(path, sha, mode='100644'):
    return {'path': path, 'mode': mode, 'type': 'blob', 'sha': sha}


def legacy_upgrade_entries(repository, parent, primary, branch, template_ref):
    template = Repository(TEMPLATE_API, repository.headers)
    if repository.read('LANGS.md', parent, optional=True) is not None:
        raise ValueError('This repository already uses a custom language layout.')
    book = repository.json('book.json', parent)
    if (book.get('root', '.') != '.' or book.get('structure', {}).get('readme', 'README.md') != 'README.md'
            or book.get('language', primary) != primary):
        raise ValueError('This repository uses a custom GitBook layout.')
    workflow = repository.read('.github/workflows/build.yml', parent, optional=True)
    if workflow is None or b'make refine' not in workflow or b'release_finish' not in workflow:
        raise ValueError('This site is not using the standard build workflow.')
    tree = repository.request('GET', f'git/trees/{parent}', params={'recursive': '1'})
    if tree.get('truncated'):
        raise ValueError('The site is too large for an automatic language upgrade.')
    files = tree['tree']
    reserved = set(LANGUAGES) | {'site-layout.json', '.bookignore', '.gitbook', '.gitbook.yaml'}
    if any(item['path'].split('/')[0] in reserved for item in files):
        raise ValueError('A language directory or site-layout.json already exists in this repository.')
    paths = {item['path'] for item in files if item['type'] == 'blob'}
    if not {'README.md', 'SUMMARY.md'}.issubset(paths):
        raise ValueError('The site needs a root README.md and SUMMARY.md before adding a language.')
    entries = {}
    for item in files:
        path = item['path']
        if path.split('/')[0] in TOOLING or item['type'] == 'tree':
            continue
        if item['type'] != 'blob' or item['mode'] not in ('100644', '100755'):
            raise ValueError(f'Unsupported site path: {path}')
        dest = f'{primary}/{path}'
        entries[dest] = entry(dest, item['sha'], item['mode'])
        entries[path] = entry(path, None, item['mode'])
    for path in ('.bookignore', 'Makefile', 'scripts/language_layout.py',
                 'scripts/subdirectory_summary.py', 'scripts/subdirectory_summary.html',
                 'scripts/subdirectory_summary_link.html', '.github/workflows/build.yml'):
        data = template.read(path, template_ref)
        if path == '.github/workflows/build.yml':
            data = data.replace(b'- master\n', f'- {branch}\n'.encode(), 2)
        entries[path] = entry(path, repository.blob(data))
    return entries


def replace_chinese_seed(repository, parent):
    """Keep the template assets when a new site starts in English."""
    tree = repository.request('GET', f'git/trees/{parent}', params={'recursive': '1'})
    if tree.get('truncated'):
        raise ValueError('Could not check the initial site template.')
    entries = {}
    for item in tree['tree']:
        path = item['path']
        if item['type'] == 'tree' or not path.startswith('zh-hans/'):
            continue
        relative = path.removeprefix('zh-hans/')
        if item['type'] != 'blob' or item['mode'] != '100644' or (
                relative not in ('README.md', 'SUMMARY.md', 'book.json') and not relative.startswith('assets/')):
            raise ValueError('The Chinese language directory already has published content.')
        entries[path] = entry(path, None)
        if relative.startswith('assets/'):
            target = 'en/' + relative
            entries[target] = entry(target, item['sha'])
    return entries


def sync(repository, branch, base_dir, parent, current, site, transform, image_bytes,
         template_ref='master', before_publish=None, upgrade_head=''):
    def path(relative):
        return posixpath.join(base_dir, relative)

    if current and current.get('version') != 2:
        raise ValueError('This repository uses a custom language layout.')
    primary = site['language']
    settings = site.get('site_languages') or {'default_language': primary, 'overrides': {}}
    upgrade = current is None
    if upgrade and (not settings['overrides'] or base_dir or not upgrade_head):
        raise ValueError('Only standard hosted sites can add languages.')
    root_language = primary if upgrade else current.get('legacy_root_language', '')
    layout = layout_for(primary, settings, root_language)
    entries = legacy_upgrade_entries(repository, parent, primary, branch, template_ref) if upgrade else {}
    if current and set(current['languages']) - layout['languages'].keys():
        if (site.get('site_language_seed_pending') != 'on' or primary != 'en'
                or root_language or set(current['languages']) != {'zh-hans'}):
            raise ValueError('Existing site languages cannot be removed.')
        entries = replace_chinese_seed(repository, parent)

    def put(relative, content):
        name = path(relative)
        if repository.read(name, parent, optional=True) != content:
            entries[name] = entry(name, repository.blob(content))

    def put_json(relative, value):
        put(relative, (json.dumps(value, indent=4, ensure_ascii=False) + '\n').encode())

    original = repository.json(path('book.json'), parent)
    common = transform(dict(site), original)
    common['pluginsConfig']['lanying-grow-ai']['sitemap_default_language'] = ''
    put_json('book.json', common)
    put_json('site-layout.json', layout)
    put('LANGS.md', ''.join(f'* [{LANGUAGES[code]}]({code}/)\n' for code in layout['languages']).encode())
    for code in layout['languages']:
        home = f'{code}/README.md'
        summary = f'{code}/SUMMARY.md'
        if code not in (current or {}).get('languages', {}) and not (upgrade and code == primary):
            if repository.read(path(code), parent, optional=True) is not None:
                raise ValueError(f'The language directory already exists: {code}')
            for filename, content in ((home, f'# {LANGUAGES[code]}\n'), (summary, f'# Summary\n\n* [{LANGUAGES[code]}](README.md)\n')):
                if repository.read(path(filename), parent, optional=True) is not None:
                    raise ValueError(f'The language directory already exists: {code}')
                put(filename, content.encode())
        elif not upgrade and repository.read(path(home), parent, optional=True) is None:
            raise ValueError(f'The language homepage is missing: {code}')
        localized = dict(site, language=code)
        if code != primary:
            localized.update(settings['overrides'][code])
        language_book = transform({**localized, 'copyright': localized.get('copyright') or ' '}, common)
        overrides = repository.json(path(f'{code}/book.json'), parent, optional=True) or {}
        # GitBook only links a stylesheet that exists inside the language directory.
        stylesheet = 'assets/styles/website.css'
        configured_style = overrides.get('styles', {}).get('website', common.get('styles', {}).get('website'))
        if code != primary and configured_style == stylesheet:
            local_style = path(f'{code}/{stylesheet}')
            shared_style = posixpath.join(root_language, stylesheet)
            source_style = path(stylesheet if upgrade else shared_style)
            if repository.read(local_style, parent, optional=True) is None and repository.read(source_style, parent, optional=True) is not None:
                relative = posixpath.relpath(shared_style, posixpath.dirname(f'{code}/{stylesheet}'))
                entries[local_style] = entry(local_style, repository.blob(f'@import url("{relative}");\n'.encode()))
        overrides = copy.deepcopy(overrides)
        overrides['language'] = code
        overrides['title'] = localized.get('title') or (common.get('title') if code == primary else '') or site['name']
        plugins = overrides.setdefault('pluginsConfig', {})
        rendered = language_book['pluginsConfig']
        for name in ('tbfed-pagefooter', 'edit-link', 'logo', 'meta', 'intopic-toc', 'lanying-grow-ai'):
            plugins[name] = copy.deepcopy(rendered.get(name, {}))
        plugins['edit-link']['base'] = rendered['edit-link']['base'] + f'/{code}'
        footer = plugins['tbfed-pagefooter']
        footer['modify_label'] = 'Updated: ' if code == 'en' else '该文件修订时间：'
        plugins['intopic-toc']['label'] = 'Contents' if code == 'en' else '内容'
        grow = plugins['lanying-grow-ai']
        for field in ('footer_note', 'lanying_link', 'hook_sentence_slogan'):
            grow[field] = localized.get(field, '')
        if plugins['meta'].get('data'):
            plugins['meta']['data'][0]['content'] = localized.get('meta_keywords', '')
        image = localized.get('hook_sentence_image', '')
        grow['hook_sentence_image'] = ''
        if image:
            data, filename = image_bytes(image)
            relative = f'assets/hook-sentence/{filename}'
            put(f'{code}/{relative}', data)
            grow['hook_sentence_image'] = relative
        put_json(f'{code}/book.json', overrides)
    if before_publish:
        before_publish()
    return repository.commit(parent, entries, branch, upgrade_head)
