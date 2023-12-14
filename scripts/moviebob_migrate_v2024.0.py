#!/usr/bin/env python3

import requests
import re
import sqlite3
from bs4 import BeautifulSoup
from logzero import logger

# --- Variables
headers = {
    "referer": "https://letterboxd.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

# --- Setup
con = sqlite3.connect("moviebob.db")
cur = con.cursor()
try:
    cur.execute("SELECT tmdb FROM movies LIMIT 1")
except sqlite3.OperationalError:
    logger.info("Column 'tmdb' not found in table. Adding column ...")
    cur.execute("ALTER TABLE movies ADD COLUMN tmdb INTEGER")

# -- Parsing
logger.info("Starting migration v2024.0")
while True:
    res = cur.execute("SELECT * FROM movies WHERE tmdb is null LIMIT 1")
    movie = res.fetchone()
    if movie is None:
        logger.info("No missing movies without tmdb entry")
        break

    title = movie[3]
    url = movie[2]
    logger.info("Parsing: %s" % title)

    try:
        soup = BeautifulSoup(requests.get(url, headers=headers).content, "html.parser")
        posterDiv = soup.find("div", attrs={"data-target-link": re.compile(r".*")})

        subUrl = posterDiv.attrs["data-target-link"]
        fullUrl = "https://letterboxd.com" + subUrl

        soup = BeautifulSoup(
            requests.get(fullUrl, headers=headers).content, "html.parser"
        )
        body = soup.find("body")
        tmdbId = body.attrs["data-tmdb-id"]

        cur.execute("UPDATE movies SET tmdb = ? WHERE url = ?", (tmdbId, url))
        con.commit()
        logger.info("Set id '%s' for '%s'" % (tmdbId, title))
    except Exception as e:
        logger.error("Error while parsing '%s': %s" % (title, e))
        continue

con.close()
logger.info("Finished migration v2024.0")
