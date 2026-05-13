"""Tests for STS / IRSA credential resolution (``tap_iceberg.aws_session``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import ClassVar, Optional
from unittest.mock import MagicMock, patch

from boto3 import Session as Boto3Session
from botocore.credentials import AssumeRoleCredentialFetcher
from botocore.credentials import CachedCredentialFetcher
from botocore.credentials import Credentials

from tap_iceberg import aws_session
from tap_iceberg.aws_session import attach_catalog_aws_credentials


def test_legacy_static_without_assume_writes_key_properties() -> None:
    catalog: dict[str, object] = {}
    attach_catalog_aws_credentials(
        catalog,
        config={
            "client_access_key_id": "AKTEST",
            "client_secret_access_key": "secret",
        },
        logger=MagicMock(),
        getenv=lambda _k: None,
    )
    assert catalog["client.access-key-id"] == "AKTEST"
    assert "botocore_session" not in catalog


def test_intermediary_then_customer_roles_order() -> None:
    class RecordingAssume(AssumeRoleCredentialFetcher):
        captures: ClassVar[list[str]] = []

        def __init__(  # type: ignore[no-untyped-def]
            self,
            client_creator,
            source_credentials,
            role_arn,
            extra_args=None,
            **kwargs,
        ):
            RecordingAssume.captures.append(role_arn)
            super().__init__(
                client_creator,
                source_credentials,
                role_arn,
                extra_args=extra_args,
                **kwargs,
            )

    RecordingAssume.captures.clear()
    canned = Credentials("IRSA-ID", "IRSA-SKEY")

    with patch.object(Boto3Session, "get_credentials", lambda self: canned):
        with patch.object(
            aws_session,
            "AssumeRoleCredentialFetcher",
            RecordingAssume,
        ):
            with patch.object(
                aws_session,
                "_wire_refreshing_pyarrow_io",
                MagicMock(),
            ):
                attach_catalog_aws_credentials(
                    {},
                    config={
                        "customer_data_access_role_arn": (
                            "arn:aws:iam::111111111111:role/ShapedCustomerData"
                        ),
                        "client_iam_role_arn": (
                            "arn:aws:iam::222222222222:role/CustomerLakeRead"
                        ),
                    },
                    logger=MagicMock(),
                    getenv=lambda _k: None,
                )

    assert RecordingAssume.captures == [
        "arn:aws:iam::111111111111:role/ShapedCustomerData",
        "arn:aws:iam::222222222222:role/CustomerLakeRead",
    ]


def test_second_get_frozen_triggers_second_assume_when_cache_bypassed() -> None:
    canned = Credentials("IRSA-ID", "IRSA-SKEY")
    calls = {"n": 0}

    def _fake_assume_identity(n: int) -> dict[str, object]:
        expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        return {
            "Credentials": {
                "AccessKeyId": f"TEMP{n}",
                "SecretAccessKey": "secret",
                "SessionToken": f"tok-{n}",
                "Expiration": expiry,
            },
            "AssumedRoleUser": {
                "Arn": f"arn:aws:sts::1:assumed-role/r{n}/sess",
            },
        }

    def tracked_get(inner_self: AssumeRoleCredentialFetcher) -> dict[str, object]:
        calls["n"] += 1
        return _fake_assume_identity(calls["n"])

    with patch.object(Boto3Session, "get_credentials", lambda self: canned):
        with patch.object(
            AssumeRoleCredentialFetcher,
            "_get_credentials",
            tracked_get,
        ):
            with patch.object(
                CachedCredentialFetcher,
                "_load_from_cache",
                return_value=None,
            ):
                with patch.object(
                    CachedCredentialFetcher,
                    "_write_to_cache",
                    lambda _self, _resp: None,
                ):
                    catalog: dict[str, object] = {}
                    attach_catalog_aws_credentials(
                        catalog,
                        config={
                            "client_iam_role_arn": (
                                "arn:aws:iam::999:role/AssumeMe"
                            ),
                        },
                        logger=MagicMock(),
                        getenv=lambda _k: None,
                    )

                    assert (
                        catalog["py-io-impl"]
                        == "tap_iceberg.refreshing_pyarrow_io.RefreshingPyArrowFileIO"
                    )
                    assert isinstance(
                        catalog["tap_iceberg.botocore_session_key"], str
                    )

                    sess_obj = catalog["botocore_session"]
                    crs = sess_obj.get_credentials()

                    crs.get_frozen_credentials()
                    # Botocore skips a second STS round-trip while creds look fresh;
                    # force advisory refresh to exercise _get_credentials again.
                    crs._expiry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
                    crs.get_frozen_credentials()

                    assert calls["n"] >= 2


def test_customer_data_arn_from_env_when_config_empty() -> None:
    class RecordingAssume(AssumeRoleCredentialFetcher):
        captures: ClassVar[list[str]] = []

        def __init__(  # type: ignore[no-untyped-def]
            self,
            client_creator,
            source_credentials,
            role_arn,
            extra_args=None,
            **kwargs,
        ):
            RecordingAssume.captures.append(role_arn)
            super().__init__(
                client_creator,
                source_credentials,
                role_arn,
                extra_args=extra_args,
                **kwargs,
            )

    RecordingAssume.captures.clear()
    canned = Credentials("IRSA-ID", "IRSA-SKEY")

    def getenv(name: str) -> Optional[str]:
        if name == "CUSTOMER_DATA_ACCESS_ROLE_ARN":
            return "arn:aws:iam::777:role/EnvCustomerDataAccess"
        return None

    with patch.object(Boto3Session, "get_credentials", lambda self: canned):
        with patch.object(
            aws_session,
            "AssumeRoleCredentialFetcher",
            RecordingAssume,
        ):
            with patch.object(
                aws_session,
                "_wire_refreshing_pyarrow_io",
                MagicMock(),
            ):
                mode = attach_catalog_aws_credentials(
                    {},
                    config={},
                    logger=MagicMock(),
                    getenv=getenv,
                )

    assert mode == aws_session.CredentialModeRefreshableIrsaCustomerDataAccessChain
    assert RecordingAssume.captures == [
        "arn:aws:iam::777:role/EnvCustomerDataAccess",
    ]


def test_customer_data_arn_env_tap_prefixed_wins_over_plain_env() -> None:
    class RecordingAssume(AssumeRoleCredentialFetcher):
        captures: ClassVar[list[str]] = []

        def __init__(  # type: ignore[no-untyped-def]
            self,
            client_creator,
            source_credentials,
            role_arn,
            extra_args=None,
            **kwargs,
        ):
            RecordingAssume.captures.append(role_arn)
            super().__init__(
                client_creator,
                source_credentials,
                role_arn,
                extra_args=extra_args,
                **kwargs,
            )

    RecordingAssume.captures.clear()
    canned = Credentials("IRSA-ID", "IRSA-SKEY")

    def getenv(name: str) -> Optional[str]:
        if name == "TAP_ICEBERG_CUSTOMER_DATA_ACCESS_ROLE_ARN":
            return "arn:aws:iam::888:role/TapPrefixedHop"
        if name == "CUSTOMER_DATA_ACCESS_ROLE_ARN":
            return "arn:aws:iam::777:role/PlainEnvHop"
        return None

    with patch.object(Boto3Session, "get_credentials", lambda self: canned):
        with patch.object(
            aws_session,
            "AssumeRoleCredentialFetcher",
            RecordingAssume,
        ):
            with patch.object(
                aws_session,
                "_wire_refreshing_pyarrow_io",
                MagicMock(),
            ):
                attach_catalog_aws_credentials(
                    {},
                    config={},
                    logger=MagicMock(),
                    getenv=getenv,
                )

    assert RecordingAssume.captures[0] == "arn:aws:iam::888:role/TapPrefixedHop"
