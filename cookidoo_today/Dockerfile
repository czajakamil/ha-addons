ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app

# Python deps
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# App
COPY app /app/app

# Runner
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
