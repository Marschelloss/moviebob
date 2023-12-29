import sqlite3
from logzero import logger
from contextlib import contextmanager


class DB:
    def __init__(self, database_path):
        self.path = database_path
        logger.debug("Setting up database...")

        with self.ops() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users(
                    user_id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    nickname TEXT NOT NULL,
                    feed_url TEXT NOT NULL
                )
            """
            )
            # ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS movies(
                    movie_id INTEGER PRIMARY KEY,
                    letterboxd_id TEXT NOT NULL UNIQUE,
                    tmdb_id INTEGER,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    rating TEXT NOT NULL,
                    rewatch INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    user INTEGER NOT NULL,
                    notified INTEGER NOT NULL
                )
            """
            )
            # Migration v2024.1
            try:
                cur.execute("SELECT tmdb_id FROM movies LIMIT 1")
            except sqlite3.OperationalError:
                logger.info("Column 'tmdb_id' not found in table. Adding column ...")
                cur.execute("ALTER TABLE movies ADD COLUMN tmdb_id INTEGER")
            # ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS monthly(
                    monthly_id INTEGER PRIMARY KEY,
                    month INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    notified INTEGER NOT NULL
                )
            """
            )
            # ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tmdb(
                    tmdb_id INTEGER PRIMARY KEY,
                    imdb_id INTEGER,
                    release_date TEXT,
                    runtime INTEGER,
                    letterboxd_avg REAL,
                    letterboxd_avg_date TEXT,
                    title TEXT NOT NULL
                )"""
            )

    @contextmanager
    def ops(self):
        con = sqlite3.connect(self.path)
        cur = con.cursor()
        yield cur
        con.commit()
        con.close()


class User:
    def __init__(self, username, db, nickname=None):
        logger.debug(f"Creating user %s ..." % username)
        self.username = username
        self.feed_url = f"https://letterboxd.com/%s/rss/" % username
        if nickname is None:
            self.nickname = username
        else:
            self.nickname = nickname
        self.db = db

        with self.db.ops() as c:
            c.execute(
                """
                 INSERT or IGNORE into users(username, nickname, feed_url)
                 VALUES (?, ?, ?)
            """,
                (self.username, self.nickname, self.feed_url),
            )

        with self.db.ops() as c:
            try:
                c.execute(
                    """
                    SELECT user_id
                    FROM users
                    WHERE username is ?
                """,
                    (self.username,),
                )
                self.user_id = c.fetchone()[0]
            except BaseException as e:
                logger.error(e)
                logger.error(f"Creation of user '%s' failed. Exiting..." % username)
                exit(1)


class Movie:
    def __init__(
        self,
        letterboxd_id,
        db,
        url,
        title,
        year,
        rating,
        date,
        user: User,
        tmdb_id=0,
        rewatch=0,
        notified=0,
    ):
        self.letterboxd_id = letterboxd_id
        self.tmdb_id = tmdb_id
        self.url = url
        self.title = title
        self.year = year
        self.rating = rating
        self.rewatch = rewatch
        self.date = date
        self.user = user
        self.notified = notified
        self.db = db

        with self.db.ops() as c:
            c.execute(
                """
                INSERT or IGNORE into movies(letterboxd_id, tmdb_id, url, title, year, rating, rewatch, date, user, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.letterboxd_id,
                    self.tmdb_id,
                    self.url,
                    self.title,
                    self.year,
                    self.rating,
                    self.rewatch,
                    self.date,
                    self.user.user_id,
                    self.notified,
                ),
            )

        with self.db.ops() as c:
            try:
                c.execute(
                    """
                    SELECT movie_id
                    FROM movies
                    WHERE letterboxd_id is ?
                """,
                    (self.letterboxd_id,),
                )
                self.movie_id = c.fetchone()[0]
            except BaseException as e:
                logger.error(e)
                logger.error(f"Creation of movie '%s' failed. Exiting..." % self.title)
                exit(1)


class TMDB:
    def __init__(
        self,
        tmdb_id,
        db,
        title,
        release_date=None,
        imdb_id=None,
        runtime=None,
        letterboxd_avg=0.0,
        letterboxd_avg_date="0",
    ):
        self.tmdb_id = tmdb_id
        self.db = db
        self.imdb_id = imdb_id
        self.title = title
        self.release_date = release_date
        self.runtime = runtime
        self.letterboxd_avg = letterboxd_avg
        self.letterboxd_avg_date = letterboxd_avg_date

        with self.db.ops() as c:
            c.execute(
                """
                INSERT or IGNORE into tmdb(tmdb_id, imdb_id, title, release_date, runtime, letterboxd_avg, letterboxd_avg_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.tmdb_id,
                    self.imdb_id,
                    self.title,
                    self.release_date,
                    self.runtime,
                    self.letterboxd_avg,
                    self.letterboxd_avg_date,
                ),
            )

        with self.db.ops() as c:
            try:
                c.execute(
                    """
                    SELECT tmdb_id
                    FROM tmdb
                    WHERE tmdb_id is ?
                """,
                    (self.tmdb_id,),
                )
                self.tmdb_id = c.fetchone()[0]
            except BaseException as e:
                logger.error(e)
                logger.error(
                    f"Creation of tmdb entry '%s' failed. Exiting..." % self.title
                )
                exit(1)
