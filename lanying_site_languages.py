"""Site language configuration and GitBook repository updates."""
import base64
import copy
import hashlib
import json
import logging
import posixpath
import re
import time
from pathlib import PurePosixPath
from urllib.parse import quote, unquote, urlsplit

import requests

REPOSITORY_VERSION = 1
LANGUAGES = {'zh-hans': '简体中文', 'en': 'English'}
COPY_FIELDS = ('title', 'meta_keywords', 'copyright', 'footer_note', 'hook_sentence_slogan',
               'hook_sentence_image', 'official_website_url', 'lanying_link')
URL_FIELDS = {'hook_sentence_image', 'official_website_url', 'lanying_link'}
# Only build tooling is copied when the administrator explicitly adds a language.
TEMPLATE_API = 'https://api.github.com/repos/maxim-top/seenical-gitbook'
TOOLING = {'.github', '.gitignore', '.bookignore', 'Makefile', 'book.json', 'scripts', 'tests', 'README-GrowAI.md'}
CSS_RELATIVE_MEDIA = re.compile(
    r'''/\*.*?\*/|url\(\s*(['"]?)(\.\./\.\./[^'")]+?)\1\s*\)''', re.IGNORECASE | re.DOTALL)
CSS_MEDIA_SUFFIXES = {'.svg', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.ico', '.avif', '.bmp',
                      '.woff', '.woff2', '.ttf', '.otf', '.eot'}


def read_repository_manifest(repository, parent):
    raw = repository.read('seenical.json', parent, optional=True)
    if raw is None:
        return {}
    manifest = json.loads(raw)
    version = manifest.get('repository_version') if isinstance(manifest, dict) else None
    if type(version) is not int or version < 0:
        raise ValueError('Invalid repository_version in seenical.json.')
    if version > REPOSITORY_VERSION:
        raise ValueError(f'This site needs a newer Connector (repository version {version}).')
    return manifest


def patch_repository_v1(repository, parent, book, manifest, put, put_json):
    """Upgrade the official footer pin and record the patch in the same Git tree."""
    plugins = book.get('plugins', [])
    footer = [item for item in plugins if item.split('@', 1)[0].lstrip('-') in ('seenical', 'lanying-grow-ai')]
    if footer not in (['seenical@1.0.0'], ['seenical@1.0.1']):
        logging.info('Skipping repository patch 1: custom footer plugin.')
        return book
    ignore = repository.read('.bookignore', parent, optional=True)
    if ignore is None:
        # Creating a new ignore file could change the build rules of a customized repository.
        logging.info('Skipping repository patch 1: the template .bookignore is missing.')
        return book
    if b'/seenical.json' not in ignore.splitlines():
        separator = b'\n' if ignore and not ignore.endswith(b'\n') else b''
        put('.bookignore', ignore + separator + b'/seenical.json\n')
    put_json('seenical.json', {**manifest, 'repository_version': 1})
    return {**book, 'plugins': ['seenical@1.0.1' if item == 'seenical@1.0.0' else item for item in plugins]}


def site_plugin_key(book):
    """Use the repository's active plugin, including legacy config-only books."""
    names = {name.split('@', 1)[0] for name in book.get('plugins', [])}
    if 'plugins' not in book:
        names = book.get('pluginsConfig', {})
    for name in ('seenical', 'lanying-grow-ai'):
        if name in names:
            return name
    return None


def localized_settings(site, code):
    fields = dict(site, language=code)
    if code != site.get('language'):
        fields.update(dict.fromkeys(COPY_FIELDS, ''))
        fields.update(site.get('site_languages', {}).get('overrides', {}).get(code, {}))
    return fields


def book_sync_state(site):
    """Remember which Console inputs reached Git, without storing tokens or copy."""
    if not site:
        return {}
    primary = site.get('language', 'zh-hans')
    keys = (*COPY_FIELDS, 'language', 'canonical_link', 'icp_number',
            'baidu_token', 'google_token', 'github_url', 'github_base_branch', 'github_base_dir')
    state = {}
    for code in [primary, *site.get('site_languages', {}).get('overrides', {})]:
        fields = localized_settings(site, code)
        fields['github_base_dir'] = (site.get('github_base_dir') or '').strip('/')
        state[code] = {k: hashlib.sha256(str(fields.get(k) or '').encode()).hexdigest() for k in keys}
    return state


def changed_fields(state, previous, code):
    before = previous.get(code, {})
    return {k for k, digest in state[code].items() if digest != before.get(k)}


def update_keywords(plugins, value, inherited=()):
    data = plugins.setdefault('meta', {}).setdefault('data', copy.deepcopy(list(inherited)))
    for item in data:
        if str(item.get('name', '')).lower() == 'keywords':
            item['content'] = value
            return
    if value:
        data.append({'name': 'keywords', 'content': value})


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


def upgrade_digest(site):
    # Identify the proposed configuration even if an ordinary settings save intervenes.
    fields = (*COPY_FIELDS, 'name', 'language', 'site_languages', 'canonical_link', 'icp_number',
              'google_token', 'baidu_token', 'github_base_dir', 'github_base_branch',
              'max_latest_num', 'commit_type', 'collaborator')
    payload = json.dumps({key: site.get(key, '') for key in fields}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def layout_for(primary, settings, legacy_root=''):
    return {
        'version': 2,
        'default_language': settings['default_language'],
        'legacy_root_language': legacy_root,
        'languages': {code: {'source_dir': code, 'public_path': '/' if code == legacy_root else f'/{code}/'}
                      for code in [primary, *settings['overrides']]}
    }


def language_options(layout, language):
    if len(layout['languages']) < 2:
        return []
    root = layout.get('legacy_root_language')
    # The old selector matches the URL. Keep its root option first so a
    # more specific language directory wins. On a mirrored homepage it
    # falls back to the first option for that language.
    root = root if root == layout['default_language'] else ''
    first = root or language
    order = [first, *(code for code in layout['languages'] if code != first)]
    return [{'value': '/' if code == root else f'/{code}/', 'text': LANGUAGES[code]} for code in order]


def release_language(layout, run, primary):
    if not layout or layout.get('version') != 2:
        return ''
    code = run.get('article_language') or primary
    if code not in LANGUAGES or code not in layout['languages']:
        raise ValueError('The article language is not enabled on the site. Add it in site settings.')
    return code


def scoped_directory(base, directory, *, reserved=()):
    parts = PurePosixPath(directory.lstrip('/')).parts
    if '..' in parts or '\\' in directory:
        raise ValueError('The publication folder must be inside its language directory.')
    if parts and parts[0] in reserved:
        raise ValueError(f'The publication folder /{parts[0]}/ is reserved for a site language. Choose a different folder.')
    return posixpath.join(base, *parts).rstrip('/') or '/'


class Repository:
    def __init__(self, api, headers):
        self.api = api
        self.headers = headers

    def request(self, method, path, *, missing_ok=False, wait_for_ref=False, **kwargs):
        attempts = 4 if wait_for_ref and method == 'GET' and path.startswith('git/ref/heads/') else 1
        for attempt in range(attempts):
            response = requests.request(method, f'{self.api}/{path}', headers=self.headers, timeout=25, **kwargs)
            if response.status_code not in (404, 409) or attempt + 1 == attempts:
                break
            time.sleep(attempt + 1)
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

    def upgrade_is_merged(self, url):
        if not url:
            return False
        number = url.rsplit('/', 1)[-1]
        pull = self.request('GET', f'pulls/{number}', missing_ok=True)
        return bool(pull and pull.get('merged'))

    def open_upgrade(self, head, base):
        owner = self.api.rsplit('/', 2)[-2]
        pulls = self.request('GET', 'pulls', params={'state': 'open', 'head': f'{owner}:{head}', 'base': base})
        return pulls[0]['html_url'] if pulls else ''

    def has_open_upgrade(self, site, base):
        head = site.get('language_upgrade_head')
        if head and self.open_upgrade(head, base):
            return True
        return self.upgrade_is_open(site.get('language_upgrade_url'))

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


def localize_assets(repository, parent, base_dir, entries, locales, existing, primary, root_language, *, upgrade):
    """Give each locale its own assets and remove only verified root duplicates."""
    scopes = {code: {} for code in ('', *locales)}
    files = {}
    prefix = base_dir.rstrip('/') + '/' if base_dir else ''

    def add_asset(item):
        if not item['path'].startswith(prefix):
            return
        name = item['path'].removeprefix(prefix)
        if item['sha'] is None:
            files.pop(item['path'], None)
        else:
            files[item['path']] = item
        code, _, relative = name.partition('/')
        if code == 'assets':
            code, relative = '', name
        if code in scopes and relative.startswith('assets/'):
            if item['sha'] is None:
                scopes[code].pop(relative, None)
            elif item['type'] == 'blob' and item['mode'] in ('100644', '100755'):
                scopes[code][relative] = item
            elif item['type'] != 'tree':
                raise ValueError(f'Unsupported site asset: {name}')

    if not upgrade:
        tree = repository.request('GET', f'git/trees/{parent}', params={'recursive': '1'})
        if tree.get('truncated'):
            raise ValueError('The site is too large to check its language assets.')
        for item in tree['tree']:
            add_asset(item)
    for item in entries.values():
        add_asset(item)

    shared = scopes['']
    stylesheet = 'assets/styles/website.css'
    standard = (stylesheet, 'assets/logo.png', 'assets/favicon.ico', 'assets/apple-touch-icon.png')
    style_sources = {}
    for code in locales:
        if stylesheet not in scopes[code]:
            inherited = '' if stylesheet in shared else primary
            if stylesheet in scopes[inherited]:
                style_sources[code] = inherited
        elif code != primary:
            target = f'{code}/{stylesheet}'
            content = repository.read(prefix + target, parent, optional=True)
            for source in ('', primary):
                relative = posixpath.relpath(posixpath.join(source, stylesheet), posixpath.dirname(target))
                if content == f'@import url("{relative}");\n'.encode():
                    inherited = source if stylesheet in scopes[source] else primary
                    if stylesheet not in scopes[inherited]:
                        raise ValueError(f'Could not find the stylesheet used by {target}.')
                    style_sources[code] = inherited
                    break

    # A copied stylesheet keeps its bytes and its direct, relative image and font references.
    media_sources = set(style_sources.values())
    if root_language and stylesheet in shared:
        media_sources.add('')
    for source in media_sources:
        source_dir = prefix + (source + '/' if source else '')
        css = repository.read(prefix + stylesheet if upgrade else source_dir + stylesheet, parent, optional=True) or b''
        for _, url in CSS_RELATIVE_MEDIA.findall(css.decode('utf-8-sig')):
            if not url:
                continue
            name = posixpath.normpath(posixpath.join(posixpath.dirname(stylesheet), unquote(urlsplit(url).path)))
            parts = PurePosixPath(name).parts
            if (not parts or parts[0] in ('assets', *LANGUAGES) or any(part.startswith('.') for part in parts)
                    or PurePosixPath(name).suffix.lower() not in CSS_MEDIA_SUFFIXES):
                continue
            if files.get(source_dir + name, {}).get('type') != 'blob':
                continue
            for code, assets in scopes.items():
                directory = prefix + (code + '/' if code else '')
                asset = files.get(directory + name)
                if asset and asset['type'] == 'blob' and asset['mode'] in ('100644', '100755'):
                    assets[name] = asset

    for code in locales:
        assets = scopes[code]
        fallback = scopes[primary]
        copies = fallback if code not in existing else {name: fallback[name] for name in standard if name in fallback}
        copies = {**copies, **shared}
        kept = {**copies, **assets}
        if code in style_sources:
            inherited = scopes[style_sources[code]]
            kept = {**copies, **inherited, **assets, stylesheet: inherited[stylesheet]}

        for name, asset in kept.items():
            for directory in PurePosixPath(name).parents:
                if directory.as_posix() in kept:
                    raise ValueError(f'The language asset {code}/{name} conflicts with a file.')
            if assets.get(name) != asset:
                target = f'{prefix}{code}/{name}'
                if target in files and name not in assets:
                    raise ValueError(f'The language asset {target} is already occupied.')
                for directory in PurePosixPath(target).parents:
                    existing_item = files.get(directory.as_posix())
                    if existing_item and existing_item['type'] != 'tree':
                        raise ValueError(f'The language asset {target} conflicts with {directory}.')
                entries[target] = entry(target, asset['sha'], asset['mode'])
        scopes[code] = kept

    # Older sites that still publish all locales under prefixes can reference root assets.
    if not root_language:
        return
    for name, asset in shared.items():
        copies = [scopes[code].get(name) for code in locales]
        if any(copy and (copy['sha'], copy['mode']) == (asset['sha'], asset['mode']) for copy in copies):
            target = prefix + name
            entries[target] = entry(target, None, asset['mode'])
        elif name in scopes[root_language]:
            raise ValueError(f'The root asset {name} conflicts with {root_language}/{name}.')
    public_assets = scopes[root_language].keys() | {name for name in shared if prefix + name not in entries}
    for name in public_assets:
        if any(directory.as_posix() in public_assets for directory in PurePosixPath(name).parents):
            raise ValueError(f'The public asset {name} conflicts with a file.')


def sync(repository, branch, base_dir, parent, current, site, transform, image_bytes,
         template_ref='master', before_publish=None, upgrade_head='', previous=None, manifest=None):
    def path(relative):
        return posixpath.join(base_dir, relative)

    if current and current.get('version') != 2:
        raise ValueError('This repository uses a custom language layout.')
    primary = site['language']
    settings = site.get('site_languages') or {'default_language': primary, 'overrides': {}}
    upgrade = current is None
    if upgrade and (not settings['overrides'] or base_dir or not upgrade_head):
        raise ValueError('Only standard hosted sites can add languages.')
    current_root = (current or {}).get('legacy_root_language', '')
    current_languages = (current or {}).get('languages', {})
    enabled_languages = {primary, *settings['overrides']}
    root_language = current_root
    entries = legacy_upgrade_entries(repository, parent, primary, branch, template_ref) if upgrade else {}
    if current_languages.keys() - enabled_languages:
        if (site.get('site_language_seed_pending') != 'on' or primary != 'en'
                or current_root not in ('', 'zh-hans') or current_languages.keys() != {'zh-hans'}):
            raise ValueError('Existing site languages cannot be removed.')
        entries = replace_chinese_seed(repository, parent)
        if current_root:
            root_language = primary
    elif upgrade or current['default_language'] != settings['default_language']:
        # A settings save that leaves the default alone preserves old public paths.
        root_language = settings['default_language']

    if current and root_language:
        reserved = enabled_languages if root_language != current_root else enabled_languages - current_languages.keys()
        for code in sorted(reserved):
            conflict = f'{root_language}/{code}'
            if repository.read(path(conflict), parent, optional=True) is not None:
                raise ValueError(f'The language path already exists: {conflict}')
    layout = layout_for(primary, settings, root_language)

    if upgrade or root_language != current_root or current_languages.keys() != layout['languages'].keys():
        localize_assets(repository, parent, base_dir, entries, layout['languages'],
                        current_languages, primary, root_language, upgrade=upgrade)

    def put(relative, content):
        name = path(relative)
        if repository.read(name, parent, optional=True) != content:
            entries[name] = entry(name, repository.blob(content))

    def put_json(relative, value):
        put(relative, (json.dumps(value, indent=4, ensure_ascii=False) + '\n').encode())

    original = repository.json(path('book.json'), parent)
    state = book_sync_state(site)
    previous = previous or {}
    primary_changes = changed_fields(state, previous, primary)
    common = transform(dict(site), original, primary_changes)
    plugin_key = site_plugin_key(original)
    if not plugin_key:
        raise ValueError('This repository does not declare a supported site plugin.')
    if not previous or upgrade:
        common['pluginsConfig'][plugin_key]['sitemap_default_language'] = ''
    has_selector = any(name.split('@', 1)[0] == 'versions-select' for name in common.get('plugins', []))
    if upgrade and 'plugins' in common and not has_selector:
        common['plugins'].append('versions-select@^0.1.1')
        has_selector = True
    versions = common['pluginsConfig'].get('versions', {})
    language_selector = has_selector and (upgrade or versions.get('type') == 'languages')
    if language_selector:
        common['pluginsConfig']['versions'] = {**versions, 'type': 'languages', 'options': language_options(layout, primary)}
    edit_link_base = common.get('pluginsConfig', {}).get('edit-link', {}).get('base', '')
    if current != layout:
        put_json('site-layout.json', {**(current or {}), **layout})
    if not current or current_languages.keys() != layout['languages'].keys():
        put('LANGS.md', ''.join(f'* [{LANGUAGES[code]}]({code}/)\n' for code in layout['languages']).encode())
    has_local_plugins = False
    for code in layout['languages']:
        home = f'{code}/README.md'
        summary = f'{code}/SUMMARY.md'
        if code not in (current or {}).get('languages', {}) and not (upgrade and code == primary):
            if repository.read(path(code), parent, optional=True) is not None:
                raise ValueError(f'The language directory already exists: {code}')
            label = LANGUAGES[code]
            if plugin_key == 'seenical':
                label = 'Documentation' if code == 'en' else '产品文档'
                seed = 'scripts/home/en.md' if code == 'en' else 'zh-hans/README.md'
                homepage = Repository(TEMPLATE_API, repository.headers).read(seed, template_ref)
            else:
                homepage = f'# {label}\n'.encode()
            for filename, content in ((home, homepage), (summary, f'# Summary\n\n* [{label}](README.md)\n'.encode())):
                if repository.read(path(filename), parent, optional=True) is not None:
                    raise ValueError(f'The language directory already exists: {code}')
                put(filename, content)
        elif not upgrade and repository.read(path(home), parent, optional=True) is None:
            raise ValueError(f'The language homepage is missing: {code}')
        localized = localized_settings(site, code)
        new_language = code not in previous or not current or code not in current['languages']
        changed = changed_fields(state, {} if new_language else previous, code)
        overrides = repository.json(path(f'{code}/book.json'), parent, optional=True) or {}
        if 'plugins' in overrides:
            has_local_plugins = True
        current_edit_link = overrides.get('pluginsConfig', {}).get('edit-link', {}).get('base')
        repair_edit_link = bool(edit_link_base and current_edit_link == f'{edit_link_base}/{code}')
        selector_options = language_options(layout, code) if language_selector else None
        selector_changed = language_selector and overrides.get('pluginsConfig', {}).get('versions', {}).get('options') != selector_options
        if not changed and not repair_edit_link and not selector_changed:
            continue
        language_book = transform({**localized, 'copyright': localized.get('copyright') or ' '}, common, changed)
        original_overrides = overrides
        overrides = copy.deepcopy(original_overrides)
        if new_language:
            overrides['language'] = code
        if 'title' in changed:
            overrides['title'] = localized.get('title') or site['name']
        plugins = overrides.setdefault('pluginsConfig', {})
        rendered = language_book['pluginsConfig']
        if selector_changed:
            plugins.setdefault('versions', {})['options'] = selector_options
        # The GitBook edit-link plugin appends the source language and filename.
        if edit_link_base and (repair_edit_link or new_language or changed & {'github_url', 'github_base_branch'}):
            plugins.setdefault('edit-link', {})['base'] = edit_link_base
        if new_language:
            plugins.setdefault('tbfed-pagefooter', {})['modify_label'] = 'Updated: ' if code == 'en' else '该文件修订时间：'
            plugins.setdefault('intopic-toc', {})['label'] = 'Contents' if code == 'en' else '内容'
        if changed & {'copyright', 'official_website_url', 'icp_number'}:
            plugins.setdefault('tbfed-pagefooter', {})['copyright'] = rendered.get('tbfed-pagefooter', {}).get('copyright', '')
        if 'official_website_url' in changed:
            plugins.setdefault('logo', {})['url'] = localized.get('official_website_url') or '/'
        if 'meta_keywords' in changed:
            update_keywords(plugins, localized.get('meta_keywords', ''), common['pluginsConfig'].get('meta', {}).get('data', []))
        grow = plugins.get(plugin_key, {}).copy()
        for field in ('footer_note', 'lanying_link', 'hook_sentence_slogan'):
            if field in changed:
                grow[field] = localized.get(field, '')
        if 'canonical_link' in changed and 'sitemap_hostname' in rendered[plugin_key]:
            grow['sitemap_hostname'] = rendered[plugin_key]['sitemap_hostname']
        if 'hook_sentence_image' in changed:
            image = localized.get('hook_sentence_image', '')
            if image:
                data, filename = image_bytes(image)
                relative = f'assets/hook-sentence/{filename}'
                put(f'{code}/{relative}', data)
                grow['hook_sentence_image'] = relative
            else:
                grow['hook_sentence_image'] = ''
        if grow:
            plugins[plugin_key] = grow
        if not plugins and 'pluginsConfig' not in original_overrides:
            overrides.pop('pluginsConfig')
        if overrides != original_overrides:
            put_json(f'{code}/book.json', overrides)
    # Only enroll an existing standard Seenical layout. Keep custom pins and disabled plugins as configured.
    if (manifest is not None and manifest.get('repository_version', 0) == 0 and not upgrade
            and plugin_key == 'seenical' and not has_local_plugins
            and original.get('root', '.') == '.' and original.get('structure', {}).get('readme', 'README.md') == 'README.md'):
        common = patch_repository_v1(repository, parent, common, manifest, put, put_json)
    if common != original:
        put_json('book.json', common)
    if before_publish:
        before_publish()
    return repository.commit(parent, entries, branch, upgrade_head)
