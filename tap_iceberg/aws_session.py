"""AWS credentials choke point for the Glue-backed Iceberg catalog.

Supports:
    * Legacy static ``client_access_key_id`` / ``client_secret_access_key`` (+ token)
    * Default-chain (IRSA) + refreshable ``sts:AssumeRole`` with optional chaining
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Mapping, MutableMapping, NamedTuple, Optional

from boto3 import Session as Boto3Session
from botocore.config import Config
from botocore.credentials import AssumeRoleCredentialFetcher
from botocore.credentials import Credentials
from botocore.credentials import DeferredRefreshableCredentials
from botocore.session import Session as BotoSession

# PyIceberg ``PyArrowFileIO`` reads these for S3 only; ``GlueCatalog`` does not use them
# for the Glue client (it uses ``botocore_session``), so we avoid overriding refreshable Glue
# credentials while fixing S3 HeadObject identity. See apache/iceberg-python ``pyarrow.py``.
_PYICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id"
_PYICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key"
_PYICEBERG_S3_SESSION_TOKEN = "s3.session-token"
_PYICEBERG_S3_REGION = "s3.region"

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


class CustomerDataAccessArnResolution(NamedTuple):
    """Resolved first-hop AssumeRole ARN and where it came from."""

    arn: Optional[str]
    """Effective ARN after config-then-env resolution."""
    source: str
    """``config``, ``env:<VAR_NAME>``, or ``none``."""


CUSTOMER_DATA_ACCESS_ENV_KEYS = (
    "TAP_ICEBERG_CUSTOMER_DATA_ACCESS_ROLE_ARN",
    "CUSTOMER_DATA_ACCESS_ROLE_ARN",
)


def _customer_data_access_arn_resolve(
    config: Mapping[str, Any],
    getenv: Callable[[str], Optional[str]],
) -> CustomerDataAccessArnResolution:
    """Resolve first-hop role (IRSA → CustomerS3DataAccessRole) ARN."""

    cfg = (config.get("customer_data_access_role_arn") or "").strip()
    if cfg:
        return CustomerDataAccessArnResolution(cfg, "config")

    for key in CUSTOMER_DATA_ACCESS_ENV_KEYS:
        cand = (getenv(key) or "").strip()
        if cand:
            return CustomerDataAccessArnResolution(cand, f"env:{key}")

    return CustomerDataAccessArnResolution(None, "none")


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


def _inject_pyiceberg_s3_credentials_from_botocore_session(
    boto_session: BotoSession,
    catalog_properties: MutableMapping[str, Any],
    *,
    client_region: Optional[str],
    logger: logging.Logger,
) -> None:
    """Mirror assumed credentials into PyIceberg S3 props.

    ``GlueCatalog`` uses ``botocore_session`` for Glue APIs, but metadata reads use
    ``PyArrowFileIO``, which ignores ``botocore_session``. Setting ``client.role-arn``
    makes PyArrow assume that role using the **pod default chain**, bypassing chained STS
    (ACCESS_DENIED on customer buckets). Snapshot frozen credentials onto ``s3.*`` keys so
    S3 matches Glue without overriding Glue ``boto3.Session`` credential kwargs.

    Botocore still refreshes credentials for Glue; **S3 reads reuse this snapshot** until
    the catalog object is recreated (typical tap runs reload once per CLI invocation).
    """

    credentials = boto_session.get_credentials()
    if credentials is None:
        logger.warning(
            "Cannot inject PyIceberg S3 credentials: botocore session has no credentials.",
        )
        return

    frozen = credentials.get_frozen_credentials()
    catalog_properties[_PYICEBERG_S3_ACCESS_KEY_ID] = frozen.access_key
    catalog_properties[_PYICEBERG_S3_SECRET_ACCESS_KEY] = frozen.secret_key
    tok = frozen.token
    if tok:
        catalog_properties[_PYICEBERG_S3_SESSION_TOKEN] = tok
    else:
        catalog_properties.pop(_PYICEBERG_S3_SESSION_TOKEN, None)
    if client_region:
        catalog_properties.setdefault(_PYICEBERG_S3_REGION, client_region)

    logger.debug(
        "Injected PyIceberg S3 credential snapshot for HeadObject reads "
        "(access_key_suffix=%s)",
        frozen.access_key[-4:] if frozen.access_key else "",
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

    Refreshable AssumeRole chains attach ``botocore_session`` for Glue APIs and mirror the
    current frozen credentials into PyIceberg ``s3.*`` properties so PyArrow S3 reads use the
    same identity (PyArrow ignores ``botocore_session`` and ``client.role-arn`` would otherwise
    re-assume using only the pod chain).

    Resolution order::

        * Legacy AKIA + secret (+ token) wins:
            assume ``client_iam_role_arn`` with static base creds, or static keys only.
        * Else ``customer_data_access_role_arn`` env:
            ``TAP_ICEBERG_CUSTOMER_DATA_ACCESS_ROLE_ARN`` or ``CUSTOMER_DATA_ACCESS_ROLE_ARN``,
            resolved with default-chain (IRSA), optional second AssumeRole hop via
            ``client_iam_role_arn`` (Glue / lake reader in the customer's account).
        * Else ``client_iam_role_arn`` alone with default chain (caller must satisfy the
          reader role trust policy).
        * Else rely on boto3 ambient defaults (**may not** reach customer buckets).
    """
    access_key = config.get("client_access_key_id")
    secret_key = config.get("client_secret_access_key")
    session_token = config.get("client_session_token")
    client_region = config.get("client_region")
    customer_arn_res = _customer_data_access_arn_resolve(config, getenv)
    customer_arn = customer_arn_res.arn
    client_iam_arn = (
        (config.get("client_iam_role_arn") or "").strip() or None
    )

    env_audit = "; ".join(
        f"{k}={'set' if bool((getenv(k) or '').strip()) else 'unset'}"
        for k in CUSTOMER_DATA_ACCESS_ENV_KEYS
    )
    logger.info(
        "Customer data access role ARN: resolution_source=%s effective_arn_suffix=%s. "
        "Env vars: %s",
        customer_arn_res.source,
        customer_arn[-12:] if customer_arn else "none",
        env_audit,
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
            _inject_pyiceberg_s3_credentials_from_botocore_session(
                boto_session,
                catalog_properties,
                client_region=client_region,
                logger=logger,
            )
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
                "First-hop intermediary role ARN is set but the default credential chain "
                "yielded no caller credentials (verify IRSA / pod identity)."
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
            fetcher_client = AssumeRoleCredentialFetcher(
                client_creator=client_creator,
                source_credentials=deferred_customer,
                role_arn=client_iam_arn,
                extra_args=_assume_extra("TapIcebergClientRole"),
            )
            final = _defer_assume(fetcher_client, method="sts-client-role")
            effective_arn = client_iam_arn
        else:
            final = deferred_customer
            effective_arn = customer_arn

        boto_session = BotoSession()
        boto_session._credentials = final  # noqa: SLF001
        catalog_properties["botocore_session"] = boto_session
        _inject_pyiceberg_s3_credentials_from_botocore_session(
            boto_session,
            catalog_properties,
            client_region=client_region,
            logger=logger,
        )

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

        logger.warning(
            "One-hop STS: ambient caller AssumeRole directly into configured "
            "client_iam_role_arn (...%s). If AssumeRole yields AccessDenied, the reader "
            "role trust likely requires chaining: set "
            "customer_data_access_role_arn (or TAP_ICEBERG_CUSTOMER_DATA_ACCESS_ROLE_ARN / "
            "CUSTOMER_DATA_ACCESS_ROLE_ARN) for the intermediary role your pod identity "
            "may assume before this reader role.",
            client_iam_arn[-12:],
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
        _inject_pyiceberg_s3_credentials_from_botocore_session(
            boto_session,
            catalog_properties,
            client_region=client_region,
            logger=logger,
        )

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
