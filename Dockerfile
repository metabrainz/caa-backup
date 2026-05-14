FROM metabrainz/python:3.13

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
                       sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN mkdir /code /data && chown www-data:www-data /data

RUN mkdir /code/caa-backup
WORKDIR /code/caa-backup

COPY pyproject.toml uv.lock /code/caa-backup/
RUN uv sync --frozen --no-dev --no-editable

COPY . /code/caa-backup
COPY ./docker/consul-template.conf /etc/consul-template.conf

EXPOSE 8080

COPY ./docker/caa-backup.service /etc/service/caa-backup/run
