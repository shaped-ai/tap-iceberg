"""AWS credentials choke point for the Glue-backed Iceberg catalog.

Supports:
    * Legacy static ``client_access_key_id`` / ``client_secret_access_key`` (+ token)
    * Default-chain (IRSA) + refreshable ``sts:AssumeRole`` with optional chaining
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Mapping, MutableMapping, Optional

from boto3 import Session as Boto3Session
from botocore.config import Config
from botocore.credentials import AssumeRoleCredentialFetcher
from botocore.credentials import Credentials
from botocore.credentials import DeferredRefreshableCredentials
from botocore.session import Session as BotoSession

STS_DURATION_SECONDS_DEFAULT = 3600

_STS_RETRY_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
)

CredentialModeLegacyEnv = "legacy_env"
CredentialModeRefreshableIrsaCustomerDataAccessChain = (
    "refreshable_irsa_customer_data_access_chain"
)
CredentialModeRefreshableIrsaClientRoleOnly = "refreshable_irsa_client_role_only"
CredentialModeDefaultChainDirectCatalog = "default_chain_direct_catalog"


def _customer_data_arn(
    config: Mapping[str, Any],
    getenv: Callable[[str], Optional[str]],
) -> Optional[str]:
    v = (
        (config.get("customer_data_access_role_arn") or "").strip()
        or (getenv("CUSTOMER_DATA_ACCESS_ROLE_ARN") or "").strip()
    )
    return v or None


def _assume_extra(role_session_name: str) -> dict[str, Any]:
    return {
        "RoleSessionName": role_session_name,
        "DurationSeconds": STS_DURATION_SECONDS_DEFAULT,
    }


def _sts_client_creator(region_name: Optional[str]) -> Callable[..., Any]:
    """Build an AssumeRole-compatible ``client_creator`` (see botocore)."""

    def _client_creator(service_name: str, **kwargs: Any) -> Any:
        merged = dict(kwargs)
        merged.setdefault("config", _STS_RETRY_CONFIG)
        if region_name:
            merged.setdefault("region_name", region_name)
        return Boto3Session().client(service_name, **merged)

    return _client_creator


def _defer_assume(
    fetcher: AssumeRoleCredentialFetcher,
    *,
    method: str,
) -> DeferredRefreshableCredentials:
    return DeferredRefreshableCredentials(
        refresh_using=fetcher.fetch_credentials,
        method=method,
    )


def attach_catalog_aws_credentials(
    catalog_properties: MutableMapping[str, Any],
    *,
    config: Mapping[str, Any],
    logger: logging.Logger,
    getenv: Callable[[str], Optional[str]] = os.getenv,
) -> str:
    """Mutate Iceberg Glue ``catalog_properties`` for AWS auth.

    Exactly one choke point for STS / boto sessions consumed by PyIceberg.
    Logs a credential mode suitable for observability (**no secrets**).

    Resolution order::

        * Legacy AKIA + secret (+ token) wins:
            assume ``client_iam_role_arn`` with static base creds, or static keys only.
        * Else ``CUSTOMER_DATA_ACCESS_ROLE_ARN`` / ``customer_data_access_role_arn``
          with default-chain base credentials (IRSA), optional second hop via
          ``client_iam_role_arn``.
        * Else ``client_iam_role_arn`` alone with default chain.
        * Else rely on boto3 ambient defaults (**may not** reach customer buckets).
    """
    access_key = config.get("client_access_key_id")
    secret_key = config.get("client_secret_access_key")
    session_token = config.get("client_session_token")
    client_region = config.get("client_region")
    customer_arn = _customer_data_arn(config, getenv)
    client_iam_arn = (
        (config.get("client_iam_role_arn") or "").strip() or None
    )

    if client_region:
        catalog_properties["client.region"] = client_region
        os.environ.setdefault("AWS_DEFAULT_REGION", client_region)

    client_creator = _sts_client_creator(client_region)

    if access_key and secret_key:
        if client_iam_arn:
            logger.info(
                "AWS credential mode: %s — refreshable STS from static caller "
                "assume role (...%s)",
                CredentialModeLegacyEnv,
                client_iam_arn[-12:],
            )
            os.environ["AWS_ACCESS_KEY_ID"] = access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
            if session_token:
                os.environ["AWS_SESSION_TOKEN"] = session_token
            else:
                os.environ.pop("AWS_SESSION_TOKEN", None)
            fetcher = AssumeRoleCredentialFetcher(
                client_creator=client_creator,
                source_credentials=Credentials(
                    access_key=access_key,
                    secret_key=secret_key,
                    token=session_token,
                ),
                role_arn=client_iam_arn,
                extra_args=_assume_extra("TapIcebergLegacyAssume"),
            )
            boto_session = BotoSession()
            boto_session._credentials = _defer_assume(  # noqa: SLF001
                fetcher,
                method="sts-assume-role-legacy-env",
            )
            catalog_properties["botocore_session"] = boto_session
            catalog_properties["client.role-arn"] = client_iam_arn
            catalog_properties["client.session-name"] = "TapIcebergLegacyAssume"
            return CredentialModeLegacyEnv

        catalog_properties["client.access-key-id"] = access_key
        catalog_properties["client.secret-access-key"] = secret_key
        if session_token:
            catalog_properties["client.session-token"] = session_token
        logger.info(
            "AWS credential mode: %s — static Glue client properties",
            CredentialModeLegacyEnv,
        )
        return CredentialModeLegacyEnv

    if customer_arn:
        base_sess = Boto3Session(region_name=client_region) if client_region else Boto3Session()
        caller = base_sess.get_credentials()
        if caller is None:
            raise ValueError(
                "CUSTOMER_DATA_ACCESS_ROLE_ARN is set but the default credential "
                "chain yielded no caller credentials (verify IRSA / pod identity)."
            )

        fetcher_customer = AssumeRoleCredentialFetcher(
            client_creator=client_creator,
            source_credentials=caller,
            role_arn=customer_arn,
            extra_args=_assume_extra("TapIcebergCustomerDataAccess"),
        )
        deferred_customer = _defer_assume(
            fetcher_customer,
            method="sts-customer-data-access",
        )

        if client_iam_arn:
            sess_name_client = "TapIcebergClientRole"
            fetcher_client = AssumeRoleCredentialFetcher(
                client_creator=client_creator,
                source_credentials=deferred_customer,
                role_arn=client_iam_arn,
                extra_args=_assume_extra(sess_name_client),
            )
            final = _defer_assume(fetcher_client, method="sts-client-role")
            effective_arn = client_iam_arn
            session_hint = sess_name_client
        else:
            sess_name_customer = "TapIcebergCustomerDataAccess"
            final = deferred_customer
            effective_arn = customer_arn
            session_hint = sess_name_customer

        boto_session = BotoSession()
        boto_session._credentials = final  # noqa: SLF001
        catalog_properties["botocore_session"] = boto_session
        catalog_properties["client.role-arn"] = effective_arn
        catalog_properties["client.session-name"] = session_hint

        logger.info(
            "AWS credential mode: %s — chained refreshable STS (effective role ...%s)",
            CredentialModeRefreshableIrsaCustomerDataAccessChain,
            effective_arn[-12:],
        )
        return CredentialModeRefreshableIrsaCustomerDataAccessChain

    if client_iam_arn:
        base_sess = (
            Boto3Session(region_name=client_region) if client_region else Boto3Session()
        )
        fetcher_src = base_sess.get_credentials()
        if fetcher_src is None:
            raise ValueError(
                "client_iam_role_arn is set without static keys "
                "and the default credential chain has no caller credentials.",
            )

        fetcher = AssumeRoleCredentialFetcher(
            client_creator=client_creator,
            source_credentials=fetcher_src,
            role_arn=client_iam_arn,
            extra_args=_assume_extra("TapIcebergCallerAssume"),
        )
        boto_session = BotoSession()
        boto_session._credentials = _defer_assume(  # noqa: SLF001
            fetcher,
            method="sts-default-chain-assume",
        )
        catalog_properties["botocore_session"] = boto_session
        catalog_properties["client.role-arn"] = client_iam_arn
        catalog_properties["client.session-name"] = "TapIcebergCallerAssume"

        logger.info(
            "AWS credential mode: %s — AssumeRole (...%s) from ambient caller",
            CredentialModeRefreshableIrsaClientRoleOnly,
            client_iam_arn[-12:],
        )
        return CredentialModeRefreshableIrsaClientRoleOnly

    logger.warning(
        "AWS credential mode: %s — neither static trio, intermediary role ARN, "
        "nor client_iam_role_arn configured; Glue may rely on boto3 defaults only.",
        CredentialModeDefaultChainDirectCatalog,
    )
    return CredentialModeDefaultChainDirectCatalog
