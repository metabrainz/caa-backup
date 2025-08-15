FROM metabrainz/python:3.13

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
                       sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /code /data && chown www-data:www-data /data
WORKDIR /code

RUN pip3.13 install setuptools

RUN mkdir /code/caa-backup
WORKDIR /code/caa-backup

COPY requirements.txt /code/caa-backup
RUN pip3.13 install -r requirements.txt

RUN apt-get autoremove -y && \
    apt-get clean -y

COPY . /code/caa-backup
COPY ./docker/consul-template.conf /etc/consul-template.conf

# Lets not start a service, but lets run commands via cron eventally.
# COPY ./docker/caa-backup.service /etc/service/caa-backup/run
