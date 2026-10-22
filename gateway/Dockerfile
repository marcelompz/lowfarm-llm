# LowFarm Gateway — container image.
#
# Python 3.12 is pinned deliberately: 3.13/3.14 are too new for some binary
# wheels (pydantic-core, redis, uvloop) and the gateway is not worth the
# risk of a source build on every deploy.

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Build deps for wheels that lack a manylinux build, then removed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
 && apt-get purge -y --auto-remove build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY app ./app
COPY mock_worker.py ./

# Run unprivileged.
RUN useradd --create-home --uid 10001 lowfarm && chown -R lowfarm:lowfarm /app
USER lowfarm

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
