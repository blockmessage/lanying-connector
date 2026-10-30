"""Read-only CDN authorization for sites owned by a paid Seenical plan.

The edge supplies the original host, never an App ID or a claimed payment state.
This endpoint is only a billing gate; existing URL signing remains in place.
Enabling it at the CDN requires the rollout checks in the payment plan.
"""
import re
import time
from urllib.parse import urlsplit

import lanying_redis
from lanying_billing_hosting import _state_hash


class SiteAccessUnavailable(RuntimeError):
    """Ownership or billing state cannot be verified; never guess an owner."""


def _hash(redis, key):
    return lanying_redis.redis_hgetall(redis, key)


def _repository(url):
    # Match existing supported GitHub forms without calling helpers that can
    # allocate a new site or write Redis while processing a public GET.
    if url.startswith('git@github.com:'):
        path = url[len('git@github.com:'):]
    else:
        parts = urlsplit(url)
        if parts.scheme != 'https' or parts.netloc != 'github.com':
            return None
        path = parts.path.lstrip('/')
    # Site configuration already accepts a trailing slash/tree path and uses
    # only the first two components. Read the same repository identity here.
    fields = path.strip('/').split('/')
    if len(fields) < 2:
        return None
    owner, repo = fields[:2]
    if repo.endswith('.git'):
        repo = repo[:-4]
    repository = owner + '/' + repo
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
        return None
    return repository


def _site(redis, app, site_id):
    if not re.fullmatch(r'[a-z0-9]{1,64}', app) or not site_id.isdigit():
        raise SiteAccessUnavailable('invalid site ownership')
    return _hash(redis, f'lanying_connector:grow_ai:site:{app}:{site_id}')


def _default_owner(redis, name):
    repository = lanying_redis.redis_hget(redis, 'lanying_connector:grow_ai:github_site_name', name)
    if not repository or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
        return None
    owner, repo = repository.split('/')
    registrations = _hash(redis, f'lanying_connector:grow_ai:github_repo_site:{owner}:{repo}')
    apps = set()
    for site_id, app in registrations.items():
        site = _site(redis, app, site_id)
        # Old reverse entries survive a site deletion/reconfiguration. Recheck
        # the current record instead of allowing them to authorize another App.
        if site.get('type') == 'gitbook' and _repository(site.get('github_url', '')) == repository:
            apps.add(app)
    if len(apps) > 1:
        raise SiteAccessUnavailable('shared site has multiple app owners')
    return next(iter(apps), None)


def _custom_owner(redis, host):
    binding = _hash(redis, 'lanying_connector:custom_domain_name:' + host)
    if not binding:
        return None
    app, site_id, domain_id = (binding.get(key, '') for key in ('app_id', 'site_id', 'domain_id'))
    if not domain_id.isdigit():
        raise SiteAccessUnavailable('invalid domain ownership')
    site = _site(redis, app, site_id)
    if not site or site.get('domain_id') != domain_id:
        return None
    domain = _hash(redis, f'lanying_connector:custom_domain_info:{app}:{site_id}:{domain_id}')
    if domain.get('domain_name', '').lower() != host or domain.get('app_id') != app or domain.get('site_id') != site_id:
        return None
    if domain.get('cdn_status', 'online') != 'online':
        return None
    return app


def allowed(host, redis=None, now=None):
    """True/False is an HTTP allow/deny, not a customer-facing billing status."""
    if not isinstance(host, str) or len(host) > 253:
        return False
    host = host.lower().rstrip('.')
    if not re.fullmatch(r'(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}', host):
        return False
    redis = redis if redis is not None else lanying_redis.get_redis_connection()
    suffix = '.site.chatai101.com'
    app = (_default_owner(redis, host[:-len(suffix)]) if host.endswith(suffix)
           else _custom_owner(redis, host))
    if app is None:
        return False
    return app_active(app, redis=redis, now=now)


def app_active(app, redis=None, now=None):
    """Paid plan gate for new uploads/publishing; legacy services stay unchanged.

    Unlike the CDN's explicitly accepted fail-open policy, mutations propagate
    storage failures and must not silently bypass an unverifiable billing state.
    """
    redis = redis if redis is not None else lanying_redis.get_redis_connection()
    state = _hash(redis, 'lanying-connector:grow_ai:service_status:' + app)
    # A legacy service is still governed by its existing billing. This gate
    # must not invent a new expiry or suspend an unrelated legacy subscription.
    if not state.get('billing_order_id'):
        return True
    if state.get('billing_writer') != 'ORDER' or state.get('billing_state_hash') != _state_hash(state):
        raise SiteAccessUnavailable('hosting billing state is inconsistent')
    if state.get('app_id') != app or state.get('status') != 'normal':
        return False
    if state.get('product_id') == '9851':
        return True  # Free hosting has no paid renewal obligation.
    try:
        end = int(state['billing_period_end_epoch'])
    except (KeyError, TypeError, ValueError) as error:
        raise SiteAccessUnavailable('hosting period is unavailable') from error
    return end > (time.time() if now is None else now)


def require_active(app):
    if not app_active(app):
        raise ValueError('plan_payment_required')
