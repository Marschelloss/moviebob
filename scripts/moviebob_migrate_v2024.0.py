#!/usr/bin/env python3

import requests
import re
import sqlite3
from bs4 import BeautifulSoup
from logzero import logger
from themoviedb import TMDb

# --- Variables
headers = {
    "referer": "https://letterboxd.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

# --- Setup
tmdb = TMDb(
    key="123",
    language="en-US",
    region="US",
)
con = sqlite3.connect("moviebob.db")
cur = con.cursor()
# TMDB
try:
    cur.execute("SELECT tmdb_id FROM movies LIMIT 1")
except sqlite3.OperationalError:
    logger.info("Column 'tmdb_id' not found in table. Adding column ...")
    cur.execute("ALTER TABLE movies ADD COLUMN tmdb_id INTEGER")

# Letterboxd AVG
try:
    cur.execute("SELECT letterboxd_avg FROM movies LIMIT 1")
except sqlite3.OperationalError:
    logger.info("Column 'letterboxd_avg' not found in table. Adding column ...")
    cur.execute("ALTER TABLE movies ADD COLUMN letterboxd_avg INTEGER")

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS tmdb(
        tmdb_id INTEGER PRIMARY KEY,
        imdb INTEGER NOT NULL,
        release_year INTEGER NOT NULL,
        runtime INTEGER NOT NULL,
        title TEXT NOT NULL
    )"""
)
con.commit()

logger.info("Starting migration v2024.0")

# -- Parsing tmdb IDs
while True:
    res = cur.execute(
        "SELECT * FROM movies tmdb_id is null OR letterboxd_avg is null LIMIT 1"
    )
    movie = res.fetchone()
    if movie is None:
        logger.info("No missing movies without tmdb entry or letterboxd_avg")
        break

    title = movie[3]
    url = movie[2]
    logger.info("Parsing '%s' with url '%s'" % (title, url))

    try:
        # Parsing fullUrl out of review url to save requests to letterboxd.com
        urlList = url.split("/")
        # last element of url is movie path
        fullUrl = "https://letterboxd.com/film/" + urlList[-2]
        logger.debug("Using url: %s" % fullUrl)

        soup = BeautifulSoup(
            requests.get(fullUrl, headers=headers).content, "html.parser"
        )
        tmdbId = soup.find("body").attrs["data-tmdb-id"]
        letterboxd_avg = (
            soup.find("meta", attrs={"name": "twitter:data2"})
            .attrs["content"]
            .split(" ")[0]
        )

        cur.execute(
            "UPDATE movies SET tmdb_id = ?, letterboxd_avg = ? WHERE url = ?",
            (tmdbId, letterboxd_avg, url),
        )
        con.commit()
        logger.info(
            "Set id '%s' and rating '%s' for '%s'" % (tmdbId, letterboxd_avg, title)
        )
    except Exception as e:
        logger.error("Error while parsing '%s': %s" % (title, e))
        continue

# -- Parsing tmdb infos
res = cur.execute("SELECT DISTINCT tmdb_id FROM movies")
movieList = res.fetchall()
for movie in movieList:
    movieId = movie[0]
    try:
        movieTmdb = tmdb.movie(movieId).details()
        cur.execute(
            "INSERT OR IGNORE into tmdb(tmdb_id, imdb, release_year, runtime, title) VALUES (?, ?, ?, ?, ?)",
            (
                movieId,
                movieTmdb.imdb_id,
                movieTmdb.year,
                movieTmdb.runtime,
                movieTmdb.title,
            ),
        )
        con.commit()
        logger.info("Parsed '%s' in database" % movieTmdb.title)
    except Exception as e:
        logger.error("Error parsing tmdb ID '%s': %s" % (movieId, e))
        continue

con.close()
logger.info("Finished migration v2024.0")
