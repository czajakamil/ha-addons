ARG BUILD_FROM
FROM $BUILD_FROM

WORKDIR /app

# Python + pip
RUN apk add --no-cache python3 py3-pip \
    && pip3 install --no-cache-dir --upgrade pip

# Jeśli w requirements masz "psycopg2" (nie binary), to dorzuć to:
# RUN apk add --no-cache gcc musl-dev postgresql-dev

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

COPY run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 8000
CMD ["/run.sh"]
