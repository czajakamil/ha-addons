ARG BUILD_FROM

FROM node:20-alpine AS frontend-build
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci || npm install
COPY frontend/tsconfig.json frontend/tsconfig.app.json frontend/tsconfig.node.json \
     frontend/vite.config.ts frontend/index.html ./
COPY frontend/src ./src
RUN npm run build

FROM ${BUILD_FROM}
WORKDIR /app

RUN apk add --no-cache build-base libffi-dev

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend/app /app/app
COPY backend/mcp_server.py /app/mcp_server.py
COPY backend/scripts /app/scripts
COPY --from=frontend-build /fe/dist /app/static

ENV MEALPILOT_DB=/data/mealpilot.db \
    MEALPILOT_STATIC_DIR=/app/static

COPY run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 8000
CMD ["/run.sh"]
