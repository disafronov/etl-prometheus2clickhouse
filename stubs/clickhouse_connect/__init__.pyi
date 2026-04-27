"""Type stubs for clickhouse-connect library.

This project uses mypy with a local `stubs/` directory (see `tool.mypy.mypy_path`).
The `clickhouse-connect` package does not currently ship type information
(no bundled stubs / no `py.typed` marker), so we provide minimal stubs for the
subset of the API used by this codebase.
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence

class QueryResult(Protocol):
    """Result returned by client.query()."""

    result_rows: Sequence[Sequence[Any]]

class Client(Protocol):
    """Minimal client surface used by this project."""

    def query(self, query: str, **kwargs: Any) -> QueryResult: ...
    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        *,
        column_names: Sequence[str] | None = ...,
        **kwargs: Any,
    ) -> None: ...

def get_client(
    *,
    host: str,
    port: int,
    username: str | None = ...,
    password: str | None = ...,
    secure: bool = ...,
    connect_timeout: int | None = ...,
    send_receive_timeout: int | None = ...,
    verify: bool | str = ...,
    **kwargs: Any,
) -> Client: ...
