ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app

# System deps for BLE (BlueZ + D-Bus) and fonts
RUN apk add --no-cache \
    bluez \
    dbus \
    ttf-dejavu

# Python deps
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# App code
COPY app/ /app/

# Startup script
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
