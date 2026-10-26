"""Opaque identities computed inside the existing provider credential boundary."""

from hashlib import sha256
import json


def option_stream_identities(settings):
    # For an array containing only strings, this encoding is RFC8785/JCS.
    # Match Cloud's existing digest([API key ID, lowercase feed]) exactly.
    # The secret is neither read nor included; this identifies a key/feed,
    # not an account entitlement or proof of an active provider connection.
    configured = (settings.options_feed or 'INDICATIVE').lower()
    return [dict(stream_identity=sha256(json.dumps([settings.api_key_id, feed],
        ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest(),
        feed=feed, provider_endpoint='ALPACA_OPTIONS_V1BETA1', configured=feed == configured)
        for feed in ('indicative', 'opra')]
