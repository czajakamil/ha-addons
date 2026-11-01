# Finance Buddy jako aplikacja Home Assistanta (nginx + uvicorn pod supervisord).
#
# Stage 1: React. Względne `base` i API, bo Ingress serwuje apkę pod
# /api/hassio_ingress/<token>/, a ten prefiks znany jest dopiero w runtime.
FROM node:20-alpine AS frontend-build

WORKDIR /app

ENV VITE_BASE=./ \
    VITE_API_BASE_URL=__api \
    VITE_DESKTOP_AUTOLOGIN=true

COPY ui/package.json ui/package-lock.json ./
RUN npm ci

COPY ui/ .
RUN npm run build

# Stage 2: Debian, nie Alpine z obrazów bazowych HA: rapidocr/onnxruntime
# i opencv nie mają kół dla musl.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-pol \
        libgl1 \
        libglib2.0-0 \
        nginx \
        supervisor \
        jq \
        sqlite3 \
    && rm -f /etc/nginx/sites-enabled/default \
    && rm -rf /var/lib/apt/lists/*

# Zależności z uv.lock. Bez `--mount=type=cache`: Supervisor nie gwarantuje BuildKitu.
COPY --from=ghcr.io/astral-sh/uv:0.9.27 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY backend ./backend

# preferences.py zapisuje na stałe do PROJECT_ROOT/data, a trwałe jest tylko /data.
RUN ln -s /data /app/data

COPY --from=frontend-build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY run.sh /run.sh
RUN chmod a+x /run.sh

ENV FINANCE_DB_PATH=/data/finance.sqlite3 \
    FINANCE_ATTACHMENTS_DIR=/data/attachments \
    FINANCE_LOG_FILE=/data/logs/finance.log \
    FINANCE_LOG_TO_CONSOLE=true \
    XDG_DATA_HOME=/data/.keyring \
    PYTHON_KEYRING_BACKEND=keyrings.alt.file.PlaintextKeyring

EXPOSE 8099

CMD ["/run.sh"]
