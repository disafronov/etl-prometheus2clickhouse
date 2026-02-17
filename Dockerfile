FROM ghcr.io/astral-sh/uv:0.10.3 AS uv

FROM ubuntu:noble-20260113 AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Etc/UTC

USER ubuntu:ubuntu

WORKDIR /home/ubuntu/app

RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/home/ubuntu/.cache/uv,uid=1000,gid=1000 \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv venv

ENV PATH="/home/ubuntu/app/.venv/bin:$PATH"

FROM base AS builder

RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/home/ubuntu/.cache/uv,uid=1000,gid=1000 \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv sync --frozen --no-install-project --link-mode=copy --no-editable --no-group dev

FROM base AS production

COPY --from=builder /home/ubuntu/app/.venv/ /home/ubuntu/app/.venv/

COPY ./ /home/ubuntu/app/

ENTRYPOINT ["python3", "main.py"]
