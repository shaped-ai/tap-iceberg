"""Refreshing PyArrow FileIO so long Iceberg syncs survive STS expiry.

PyIceberg's default :class:`pyiceberg.io.pyarrow.PyArrowFileIO` creates a
:class:`pyarrow.fs.S3FileSystem` once from ``s3.access-key-id`` /
``s3.secret-access-key`` / ``s3.session-token`` strings. The underlying C++
AWS SDK does not refresh those, so any sync that outlives the assumed-role
``DurationSeconds`` (~1h with our STS settings, jittered by botocore) fails
with HTTP 400/UNKNOWN HeadObject errors even when ``botocore_session`` keeps
refreshing fine for Glue calls.

``RefreshingPyArrowFileIO`` keeps the same ``botocore.session.Session`` we
already use for Glue and rebuilds the S3 client whenever current credentials
fall inside a small refresh window before expiry. PyIceberg can only pass
string properties through ``load_catalog``, so the session is exchanged via
``register_botocore_session`` + a token written to ``catalog_properties``.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

from pyarrow.fs import FileSystem, S3FileSystem
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties

logger = logging.getLogger(__name__)

SESSION_KEY_PROPERTY = "tap_iceberg.botocore_session_key"
"""Catalog property name that holds the registry key for the boto session."""

PY_IO_IMPL_PATH = "tap_iceberg.refreshing_pyarrow_io.RefreshingPyArrowFileIO"
"""Fully-qualified path PyIceberg loads as :class:`PyArrowFileIO` replacement."""

_REFRESH_WINDOW_SECONDS = 300
"""Rebuild ``S3FileSystem`` if its credentials expire within this many seconds."""

_STATIC_CACHE_TTL_SECONDS = 60 * 60 * 24
"""Cache static credentials (no expiry attached) for a long, finite TTL."""

_REGISTRY_LOCK = threading.Lock()
_BOTOCORE_SESSION_REGISTRY: Dict[str, Any] = {}


def register_botocore_session(session: Any) -> str:
    """Register a ``botocore`` session and return a registry token.

    The token is written to ``catalog_properties`` so PyIceberg (which only
    accepts string properties) can hand it back to the FileIO at construction
    time. Sessions are intentionally **not** removed automatically — taps run
    in their own short-lived process and a global dict avoids life-cycle
    coupling with PyIceberg's catalog/FileIO objects.
    """
    key = uuid.uuid4().hex
    with _REGISTRY_LOCK:
        _BOTOCORE_SESSION_REGISTRY[key] = session
    return key


def _lookup_botocore_session(key: Optional[str]) -> Optional[Any]:
    if not key:
        return None
    with _REGISTRY_LOCK:
        return _BOTOCORE_SESSION_REGISTRY.get(key)


def _expiry_to_epoch(expiry: Any) -> Optional[float]:
    if expiry is None:
        return None
    if isinstance(expiry, datetime):
        return expiry.timestamp()
    try:
        return float(expiry)
    except (TypeError, ValueError):
        return None


class RefreshingPyArrowFileIO(PyArrowFileIO):
    """``PyArrowFileIO`` that rebuilds ``S3FileSystem`` from refreshable STS creds.

    Non-S3 schemes (``file://``, ``hdfs://``, ``gs://``) fall through to
    upstream ``PyArrowFileIO`` behavior. S3 schemes go through
    :meth:`_build_or_get_s3_fs`, which is invoked on every file open via the
    overridden ``fs_by_scheme`` (we cannot reuse the parent ``lru_cache``
    because it would pin a stale ``S3FileSystem`` for the life of the IO).
    """

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        super().__init__(properties=properties)
        self._session_key: Optional[str] = properties.get(SESSION_KEY_PROPERTY)
        self._s3_fs_cache: Dict[Tuple[str, Optional[str]], Tuple[float, S3FileSystem]] = {}
        self._fs_lock = threading.Lock()

        parent_fs_by_scheme = self.fs_by_scheme

        def refreshing_fs_by_scheme(
            scheme: str,
            netloc: Optional[str] = None,
        ) -> FileSystem:
            if scheme in {"s3", "s3a", "s3n"} and self._session_key:
                return self._build_or_get_s3_fs(scheme, netloc)
            return parent_fs_by_scheme(scheme, netloc)

        self.fs_by_scheme = refreshing_fs_by_scheme  # type: ignore[assignment]

    def _build_or_get_s3_fs(
        self,
        scheme: str,
        netloc: Optional[str],
    ) -> S3FileSystem:
        cache_key = (scheme, netloc)
        now = time.time()

        with self._fs_lock:
            session = _lookup_botocore_session(self._session_key)
            if session is None:
                logger.warning(
                    "RefreshingPyArrowFileIO: no boto session registered for key=%s; "
                    "falling back to upstream PyArrowFileIO behavior for %s://",
                    self._session_key,
                    scheme,
                )
                return super()._initialize_fs(scheme, netloc)

            credentials = session.get_credentials()
            if credentials is None:
                logger.warning(
                    "RefreshingPyArrowFileIO: boto session yielded no credentials; "
                    "falling back to upstream PyArrowFileIO behavior for %s://",
                    scheme,
                )
                return super()._initialize_fs(scheme, netloc)

            # ``get_frozen_credentials`` lets botocore refresh under the hood if the
            # credentials are within its own advisory window; we then read the
            # (possibly updated) ``_expiry_time`` to decide whether to reuse a cached
            # ``S3FileSystem`` or build a fresh one with the new frozen tuple.
            frozen = credentials.get_frozen_credentials()
            current_expiry = _expiry_to_epoch(getattr(credentials, "_expiry_time", None))
            if current_expiry is None:
                current_expiry = now + _STATIC_CACHE_TTL_SECONDS

            cached = self._s3_fs_cache.get(cache_key)
            if (
                cached is not None
                and cached[0] == current_expiry
                and current_expiry > now + _REFRESH_WINDOW_SECONDS
            ):
                return cached[1]

            s3_fs = self._construct_s3_fs(
                access_key=frozen.access_key,
                secret_key=frozen.secret_key,
                session_token=frozen.token,
            )

            self._s3_fs_cache[cache_key] = (current_expiry, s3_fs)
            logger.debug(
                "RefreshingPyArrowFileIO: built fresh S3FileSystem "
                "(scheme=%s netloc=%s key_suffix=%s expires_in_s=%d)",
                scheme,
                netloc,
                frozen.access_key[-4:] if frozen.access_key else "",
                max(0, int(current_expiry - now)),
            )
            return s3_fs

    def _construct_s3_fs(
        self,
        *,
        access_key: Optional[str],
        secret_key: Optional[str],
        session_token: Optional[str],
    ) -> S3FileSystem:
        kwargs: Dict[str, Any] = {
            "access_key": access_key,
            "secret_key": secret_key,
        }
        if session_token:
            kwargs["session_token"] = session_token

        region = self.properties.get("s3.region") or self.properties.get("client.region")
        if region:
            kwargs["region"] = region

        endpoint = self.properties.get("s3.endpoint")
        if endpoint:
            kwargs["endpoint_override"] = endpoint

        proxy = self.properties.get("s3.proxy-uri")
        if proxy:
            kwargs["proxy_options"] = proxy

        connect_timeout = self.properties.get("s3.connect-timeout")
        if connect_timeout:
            try:
                kwargs["connect_timeout"] = float(connect_timeout)
            except (TypeError, ValueError):
                pass

        return S3FileSystem(**kwargs)


def attach_refreshing_pyarrow_io(
    catalog_properties: Dict[str, Any],
    boto_session: Any,
    *,
    client_region: Optional[str] = None,
    register: Callable[[Any], str] = register_botocore_session,
) -> str:
    """Wire ``catalog_properties`` so PyIceberg uses our refreshing FileIO.

    Returns the registry key, mostly for tests / observability.
    """
    key = register(boto_session)
    catalog_properties["py-io-impl"] = PY_IO_IMPL_PATH
    catalog_properties[SESSION_KEY_PROPERTY] = key
    if client_region:
        catalog_properties.setdefault("s3.region", client_region)
    return key
