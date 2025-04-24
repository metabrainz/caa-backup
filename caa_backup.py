#!/usr/bin/env python3

import io
import os
import sys
from time import sleep
import concurrent.futures

import psycopg2
from psycopg2.extras import execute_values
import psycopg2.errors
import requests
from tqdm import tqdm

import config

def load_coverart_helper(obj, caa_id, release_mbid, mime_type):
    return obj.fetch(caa_id, release_mbid, mime_type)

class CoverArtLoader:

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir


    def cache_path(self, release_mbid, caa_id, mime_type):
        """ Given a release_mbid, create the file system path to where the cover art should be saved and 
            ensure that the directory for it exists. """

        ext = mime_type.split("/")[1]
        path = os.path.join(self.cache_dir, release_mbid[0], release_mbid[0:1], release_mbid[0:2])
        try:
            os.makedirs(path)
        except FileExistsError:
            pass
        return os.path.join(path, f"{release_mbid}_{caa_id}.{ext}")


    def _download_file(self, url):
        """ Download a file given a URL and return that file as file-like object. """

        sleep_duration = 2
        while True:
            headers = {'User-Agent': 'Cover Art Archive Backup ( rob@metabrainz.org )'}
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                total = 0
                obj = io.BytesIO()
                for chunk in r:
                    total += len(chunk)
                    obj.write(chunk)
                obj.seek(0, 0)
                return obj, ""

            if r.status_code in [403, 404]:
                return None, f"Could not load resource: {r.status_code}."

            if r.status_code == 429:
                print("Exceeded rate limit. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None, "Timeout loading image, due to 429"

                continue

            if r.status_code == 503:
                print("Service not available. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None, "Timeout loading image, 503"
                continue

            return None, "Unhandled status code: %d" % r.status_code

    def _download_cover_art(self, release_mbid, caa_id, cover_art_file):
        """ The cover art for the given release mbid does not exist, so download it,
            save a local copy of it. """

        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb250.jpg"
        image, err = self._download_file(url)
        if image is None:
            return err

        with open(cover_art_file, 'wb') as f:
            f.write(image.read())

        return None

    def fetch(self, caa_id, release_mbid, mime_type):
        cover_art_file = self.cache_path(release_mbid, caa_id, mime_type)
        if not os.path.exists(cover_art_file):
            err = self._download_cover_art(release_mbid, caa_id, cover_art_file)
            if err is not None:
                return release_mbid, "download failed"
            else:
                return release_mbid, "ok"

        return release_mbid, "exists"


    def fetch_all(self):

        releases = []
        query = """SELECT r.gid AS release_mbid 
                        , ca.id AS caa_id
                        , mime_type
                     FROM cover_art_archive.cover_art ca
                     JOIN release r
                       ON ca.release = r.id
                 ORDER BY r.gid"""

        with psycopg2.connect(config.MBID_MAPPING_DATABASE_URI) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute(query)
                for row in curs:
                    releases.append((row["release_mbid"], row["caa_id"], row["mime_type"]))

        print("%d releases" % len(releases))

        return releases

    def load_images(self):

        release_colors = {}
        print("fetch release list from db")
        releases = self.fetch_all()
        print("load images")
        calculated = 0
        with tqdm(total=len(releases)) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                futures = {executor.submit(load_coverart_helper, self, release[1], release[0], release[2]) :
                           release
                           for release in releases}
                for future in concurrent.futures.as_completed(futures):
                    release = futures[future]
                    try:
                        release_mbid, msg = future.result()
                        tqdm.write(f"{release_mbid}: {msg}")
                    except Exception as exc:
                        tqdm.write('%s generated an exception: %s' % (release_mbid, exc))
                    pbar.update(1)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: caa_backup.py <cache_dir>")
        sys.exit(-1)

    cal = CoverArtLoader(sys.argv[1])
    cal.load_images()
