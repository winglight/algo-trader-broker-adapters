"""Shared IB gateway configuration objects."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

__all__ = ["IBGatewaySettings"]


@dataclass(slots=True)
class IBGatewaySettings:
    """Connection configuration for the IB Gateway or Trader Workstation."""

    host: str = "127.0.0.1"
    host_fallbacks: tuple[str, ...] = ()
    port: int = 4001
    client_id: int = 1
    client_id_fallbacks: tuple[int, ...] = ()
    connect_timeout: float = 15.0
    read_only: bool = False
    account: str | None = None
    market_data_depth_rows: int = 10
    market_data_queue_size: int = 64
    historical_timeout: float = 60.0

    def validate(self) -> None:
        """Validate settings to guard against invalid configuration."""

        if not self.host:
            raise ValueError("IB gateway host must be provided")
        seen_hosts: set[str] = set()
        primary_host = self.host.strip()
        for fallback_host in self.host_fallbacks:
            normalized_host = fallback_host.strip()
            if not normalized_host:
                raise ValueError("IB host fallbacks must not contain empty values")
            lowered = normalized_host.lower()
            if lowered == primary_host.lower() or lowered in seen_hosts:
                raise ValueError(
                    "IB host fallbacks must be unique and differ from the primary host"
                )
            seen_hosts.add(lowered)
        if self.port <= 0:
            raise ValueError("IB gateway port must be positive")
        if self.client_id < 0:
            raise ValueError("IB client id must be non-negative")
        seen_ids: set[int] = set()
        for fallback in self.client_id_fallbacks:
            if fallback < 0:
                raise ValueError("IB client id fallbacks must be non-negative")
            if fallback in seen_ids or fallback == self.client_id:
                raise ValueError(
                    "IB client id fallbacks must be unique and differ from the primary client id"
                )
            seen_ids.add(fallback)
        if self.connect_timeout <= 0:
            raise ValueError("Connection timeout must be positive")
        if self.market_data_depth_rows <= 0:
            raise ValueError("Market depth rows must be positive")
        if self.market_data_queue_size <= 0:
            raise ValueError("Market data queue size must be positive")
        if self.historical_timeout <= 0:
            raise ValueError("Historical request timeout must be positive")

    @classmethod
    def from_env(
        cls,
        prefix: str = "IB_",
        env: Mapping[str, str] | None = None,
    ) -> "IBGatewaySettings":
        """Construct settings from environment variables."""

        source = env or os.environ
        mapping = {
            "host": str,
            "port": int,
            "client_id": int,
            "connect_timeout": float,
            "read_only": _parse_bool,
            "account": str,
            "market_data_depth_rows": int,
            "market_data_queue_size": int,
            "historical_timeout": float,
        }

        kwargs: dict[str, object] = {}
        for field, caster in mapping.items():
            env_key = f"{prefix}{field.upper()}"
            value = source.get(env_key)
            if not value:
                gateway_key = f"{prefix}GATEWAY_{field.upper()}"
                value = source.get(gateway_key)
            if value is None or value == "":
                continue
            kwargs[field] = caster(value)

        fallback_key = f"{prefix}CLIENT_ID_FALLBACKS"
        fallback_value = source.get(fallback_key)
        if not fallback_value:
            gateway_key = f"{prefix}GATEWAY_CLIENT_ID_FALLBACKS"
            fallback_value = source.get(gateway_key)
        if fallback_value:
            kwargs["client_id_fallbacks"] = _parse_client_id_fallbacks(fallback_value)

        host_fallback_key = f"{prefix}HOST_FALLBACKS"
        host_fallback_value = source.get(host_fallback_key)
        if not host_fallback_value:
            gateway_key = f"{prefix}GATEWAY_HOST_FALLBACKS"
            host_fallback_value = source.get(gateway_key)
        if host_fallback_value:
            kwargs["host_fallbacks"] = _parse_host_fallbacks(host_fallback_value)
        elif str(kwargs.get("host") or "").strip().lower() == "ib-gateway":
            # Support mixed local/container deployments where `ib-gateway` DNS can be transient.
            kwargs["host_fallbacks"] = ("host.docker.internal", "127.0.0.1")

        settings = cls(**kwargs)  # type: ignore[arg-type]
        settings.validate()
        return settings


def _parse_bool(value: str) -> bool:
    truthy = {"1", "true", "yes", "on", "y", "t"}
    falsy = {"0", "false", "no", "off", "n", "f"}
    lowered = value.strip().lower()
    if lowered in truthy:
        return True
    if lowered in falsy:
        return False
    raise ValueError(f"Unable to parse boolean value from '{value}'")


def _parse_client_id_fallbacks(value: str) -> tuple[int, ...]:
    items = []
    for token in value.split(","):
        candidate = token.strip()
        if not candidate:
            continue
        try:
            parsed = int(candidate)
        except ValueError:
            continue
        if parsed < 0:
            continue
        if parsed in items:
            continue
        items.append(parsed)
    return tuple(items)


def _parse_host_fallbacks(value: str) -> tuple[str, ...]:
    items: list[str] = []
    seen: set[str] = set()
    for token in value.split(","):
        candidate = token.strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        items.append(candidate)
    return tuple(items)
