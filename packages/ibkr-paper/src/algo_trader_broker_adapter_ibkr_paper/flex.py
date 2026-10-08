"""Read-only IB Activity Flex transport. Tokens never enter evidence or errors."""

import asyncio
from collections import deque
from hashlib import sha256
import time
from urllib.parse import urlencode
from urllib.request import Request, build_opener, HTTPRedirectHandler
from xml.etree import ElementTree as ET

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.options import check

MAX_BYTES = 8 * 1024 * 1024
BASE = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/"
PENDING = {"1001", "1003", "1004", "1005", "1006", "1007", "1008", "1009", "1019", "1021"}
_GOVERNORS = {}


class _Tree(ET.TreeBuilder):
    def __init__(self):
        super().__init__()
        self.depth = self.nodes = 0

    def doctype(self, *args):
        raise BrokerContractError("Flex XML must not contain a DTD")

    def start(self, tag, attrs):
        self.depth += 1
        self.nodes += 1
        check(self.depth <= 32 and self.nodes <= 100000, "Flex XML structure exceeds limits")
        return super().start(tag, attrs)

    def end(self, tag):
        self.depth -= 1
        return super().end(tag)


def xml(raw):
    check(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, "Flex XML size is invalid")
    try:
        return ET.fromstring(raw, parser=ET.XMLParser(target=_Tree()))
    except (ET.ParseError, ValueError):
        raise BrokerContractError("Invalid Flex XML") from None


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def _download(endpoint, token, reference):
    # Do not use the URL returned in the XML, log requests, or propagate a
    # urllib exception: its URL contains the Flex credential.
    request = Request(BASE + endpoint + "?" + urlencode(dict(t=token, q=reference, v=3)),
        headers={"User-Agent": "ATI-IB-Flex/1 Python-urllib", "Accept": "application/xml"})
    try:
        with build_opener(_NoRedirect()).open(request, timeout=15) as response:
            raw = response.read(MAX_BYTES + 1)
            check(len(raw) <= MAX_BYTES, "Flex response exceeds size limit")
            return raw
    except BrokerContractError:
        raise
    except Exception:
        raise BrokerConnectionError("IB Flex download failed") from None


class _Governor:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.calls = deque(maxlen=10)
        self.blocked_until = 0.0

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            due = max(self.blocked_until, self.calls[-1] + 1.01 if self.calls else now,
                self.calls[0] + 60.01 if len(self.calls) == 10 else now)
            if due > now:
                await asyncio.sleep(due - now)
            self.calls.append(time.monotonic())


class FlexReader:
    def __init__(self, settings):
        self._token = str(settings.get("ib_flex_token") or "")
        self.query = str(settings.get("ib_flex_query_id") or "")
        accounts = settings.get("ib_flex_accounts") or ""
        self.accounts = frozenset(item.strip() for item in accounts.split(",") if item.strip()) if isinstance(accounts, str) else frozenset(accounts)
        self.lock = asyncio.Lock()
        self.reference = None
        self.next_poll = 0.0
        self.cached = None
        self.cached_until = 0.0
        self.source_key = sha256((self.query + ":" + self._token).encode()).hexdigest()

    @property
    def configured(self):
        return bool(self._token and self.query and self.accounts)

    async def fetch(self, account, retain, *, state_store=None):
        if not self.configured:
            raise BrokerCapabilityError("IB Flex query, secret and account allowlist are required", code="IB_FLEX_UNCONFIGURED")
        check(account in self.accounts and self.query.isdecimal(), "IB Flex account or query is invalid")
        async with self.lock:
            if self.cached is not None and time.monotonic() < self.cached_until:
                return self.cached
            if state_store is not None and self.reference is None:
                self.reference = await state_store.load(self.source_key)
            if time.monotonic() < self.next_poll:
                return None
            governor = _GOVERNORS.setdefault(sha256(self._token.encode()).hexdigest(), _Governor())
            # One generation and at most two statement polls per invocation.
            for _ in range(3):
                endpoint = "GetStatement" if self.reference else "SendRequest"
                await governor.acquire()
                raw = await asyncio.to_thread(_download, endpoint, self._token, self.reference or self.query)
                root = xml(raw)
                if root.tag == "FlexQueryResponse":
                    check(endpoint == "GetStatement", "Unexpected Flex statement response")
                    check(await retain(raw) == sha256(raw).hexdigest(), "Flex archive changed source bytes")
                    from .options_lifecycle import document
                    try:
                        document(raw, account, self.accounts)
                    except (BrokerContractError, ValueError, KeyError, TypeError):
                        self.next_poll = time.monotonic() + 5
                        return raw
                    # Keep the reference until the bounded statement is durably
                    # retained. The parser independently validates its coverage.
                    if state_store is not None:
                        await state_store.save(self.source_key, None)
                    self.reference = None
                    self.cached, self.cached_until = raw, time.monotonic() + 60
                    return raw
                check(root.tag == "FlexStatementResponse", "Unexpected Flex response type")
                status = root.findtext("Status")
                if status == "Success":
                    reference = root.findtext("ReferenceCode")
                    check(endpoint == "SendRequest" and reference is not None and reference.isdecimal()
                          and len(reference) <= 128, "Invalid Flex reference")
                    self.reference = reference
                    if state_store is not None:
                        await state_store.save(self.source_key, reference)
                    continue
                code = root.findtext("ErrorCode")
                check(status in {"Fail", "Warn"} and code is not None and code.isdecimal(), "Invalid Flex error response")
                if code == "1018":
                    governor.blocked_until = time.monotonic() + 60
                    self.next_poll = governor.blocked_until
                    return None
                if code in PENDING:
                    self.next_poll = time.monotonic() + 5
                    return None
                # Only the numeric code is safe to expose. Messages may echo
                # submitted credentials; a bad reference is not auto-regenerated.
                raise BrokerCapabilityError("IB Flex configuration requires attention (" + code + ")", code="IB_FLEX_CONFIGURATION_INVALID")
            self.next_poll = time.monotonic() + 5
            return None
