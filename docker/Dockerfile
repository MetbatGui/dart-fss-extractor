FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/app/.venv/bin:$PATH \
    TZ=Asia/Seoul

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev \
    && apt-get update \
    && apt-get install -y --no-install-recommends cron tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && chmod 0644 /app/docker/crontab \
    && chmod +x /app/docker/*.sh \
    && chown -R nonroot:nonroot /app

COPY docker/crontab /etc/cron.d/dart-fss-extractor
RUN chmod 0644 /etc/cron.d/dart-fss-extractor

ENTRYPOINT []
USER nonroot
CMD ["python", "src/main.py", "--help"]
