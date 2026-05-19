"""Tests for ``tap_iceberg.refreshing_pyarrow_io.RefreshingPyArrowFileIO``."""

from __future__ import annotations

import time
from typing import Any, Optional
from unittest.mock import patch

from tap_iceberg import refreshing_pyarrow_io


class _FrozenCreds:
    def __init__(self, access_key: str, secret_key: str, token: Optional[str]) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token


class _StubRefreshableCredentials:
    """Mimic the bits of ``botocore.credentials.RefreshableCredentials`` we use."""

    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        token: Optional[str],
        expiry_time: Optional[float],
    ) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token
        self._expiry_time: Optional[float] = expiry_time

    def get_frozen_credentials(self) -> _FrozenCreds:
        return _FrozenCreds(self.access_key, self.secret_key, self.token)


class _StubBotoSession:
    def __init__(self, credentials: _StubRefreshableCredentials) -> None:
        self.credentials = credentials

    def get_credentials(self) -> _StubRefreshableCredentials:
        return self.credentials


def _capture_s3_kwargs() -> tuple[list[dict[str, Any]], Any]:
    calls: list[dict[str, Any]] = []

    class _RecordingS3FileSystem:
        def __init__(self, **kwargs: Any) -> None:
            calls.append(kwargs)
            self.kwargs = kwargs

    return calls, _RecordingS3FileSystem


def test_refreshing_pyarrow_io_rebuilds_after_simulated_sts_refresh() -> None:
    creds = _StubRefreshableCredentials(
        access_key="AK1",
        secret_key="secret",
        token="tok1",
        expiry_time=time.time() + 3600,
    )
    boto_session = _StubBotoSession(creds)
    key = refreshing_pyarrow_io.register_botocore_session(boto_session)

    calls, recording_cls = _capture_s3_kwargs()

    with patch.object(refreshing_pyarrow_io, "S3FileSystem", recording_cls):
        io = refreshing_pyarrow_io.RefreshingPyArrowFileIO(
            properties={
                refreshing_pyarrow_io.SESSION_KEY_PROPERTY: key,
                "s3.region": "us-east-1",
            },
        )

        fs1 = io.fs_by_scheme("s3", "bucket")
        fs2 = io.fs_by_scheme("s3", "bucket")
        assert fs1 is fs2  # cache hit while creds remain fresh

        creds.access_key = "AK2"
        creds.token = "tok2"
        creds._expiry_time = time.time() + 7200

        fs3 = io.fs_by_scheme("s3", "bucket")
        assert fs3 is not fs1  # rebuilt after simulated STS refresh

    assert [c["access_key"] for c in calls] == ["AK1", "AK2"]
    assert calls[0]["region"] == "us-east-1"
    assert calls[1]["session_token"] == "tok2"


def test_refreshing_pyarrow_io_rebuilds_when_inside_refresh_window() -> None:
    creds = _StubRefreshableCredentials(
        access_key="AK1",
        secret_key="secret",
        token="tok1",
        expiry_time=time.time() + 3600,
    )
    boto_session = _StubBotoSession(creds)
    key = refreshing_pyarrow_io.register_botocore_session(boto_session)

    calls, recording_cls = _capture_s3_kwargs()

    with patch.object(refreshing_pyarrow_io, "S3FileSystem", recording_cls):
        io = refreshing_pyarrow_io.RefreshingPyArrowFileIO(
            properties={refreshing_pyarrow_io.SESSION_KEY_PROPERTY: key},
        )
        first = io.fs_by_scheme("s3", "bucket")
        creds._expiry_time = time.time() + 60  # inside 5-min refresh window
        second = io.fs_by_scheme("s3", "bucket")

    assert first is not second
    assert len(calls) == 2


def test_refreshing_pyarrow_io_falls_back_when_no_session_registered() -> None:
    io = refreshing_pyarrow_io.RefreshingPyArrowFileIO(properties={})
    assert io._session_key is None
