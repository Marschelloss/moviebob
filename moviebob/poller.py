import sqlite3
from bs4 import BeautifulSoup
import feedparser
from logzero import logger
import requests
from moviebob import helper
from datetime import datetime
from time import mktime

headers = {
    "referer": "https://letterboxd.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}


def setup_users(user_list, db):
    """
    Parses provided user_list and creates corresponding entries in the database
    :param user_list:
    :param db:
    :return:
    """
    parsed_user_list = {}
    for user in user_list:
        parsed_string = user.split(":")
        parsed_user = ""
        if len(parsed_string) == 1:
            parsed_user = helper.User(username=parsed_string[0], db=db)
        elif len(parsed_string) == 2:
            parsed_user = helper.User(
                username=parsed_string[0], nickname=parsed_string[1], db=db
            )
        else:
            logger.error(
                f"Could not parse provided user string '%s'. Need format 'username:nickname'! Exiting ..."
                % user
            )
            exit(1)
        parsed_user_list[parsed_user.username] = parsed_user
    return parsed_user_list


def fetch_letterboxd_avg(soup, fullUrl):
    # Average rating from website parsing (probably brakes one day)
    try:
        letterboxdAvg = float(
            soup.find("meta", attrs={"name": "twitter:data2"})
            .attrs["content"]
            .split(" ")[0]
        )
        logger.debug("Parsed average rating '%s' from '%s'." % (letterboxdAvg, fullUrl))
        return letterboxdAvg
    except Exception as e:
        logger.debug("Not able to parse average rating from '%s': %s" % (fullUrl, e))
        return 0


def update_letterboxd_avg(db: helper.DB):
    logger.debug("Updating letterboxd average for every movie older than 30 days ...")
    movie_list = []
    with db.ops() as c:
        c.execute(
            """
            SELECT title, tmdb_id, letterboxd_avg
            FROM tmdb
            WHERE date(letterboxd_avg_date) <= date('now', '-1 day')
        """
        )
        movie_list = c.fetchall()

    for movie in movie_list:
        title = movie[0]
        tmdbId = movie[1]
        letterboxdAvg = movie[2]
        fullUrl = ""

        try:
            # Get letterboxd url from different table
            with db.ops() as c:
                c.execute(
                    "SELECT url, rewatch FROM movies WHERE tmdb_id = ?", (tmdbId,)
                )
                e = c.fetchone()
                urlList = e[0].split("/")
                # Remove empty fields from list
                urlList = list(filter(None, urlList))
                # If rewatch, a number is added to the url
                if e[1]:
                    fullUrl = "https://letterboxd.com/film/" + urlList[-2]
                else:
                    fullUrl = "https://letterboxd.com/film/" + urlList[-1]

            logger.debug("Using fullUrl: '%s'" % fullUrl)
            soup = BeautifulSoup(
                requests.get(fullUrl, headers=headers).content,
                "html.parser",
            )
            letterboxdAvgNew = fetch_letterboxd_avg(soup, fullUrl)
            if letterboxdAvg != letterboxdAvgNew:
                logger.debug(
                    "Letterboxd average changed for '%s' from %s to %s"
                    % (title, letterboxdAvg, letterboxdAvgNew)
                )
            else:
                logger.debug("Letterboxd average did not change for '%s'" % title)
            # Update row regardless to update timestamp
            timestamp = datetime.now().isoformat()
            with db.ops() as c:
                c.execute(
                    "UPDATE tmdb SET letterboxd_avg = ?, letterboxd_avg_date = ? WHERE tmdb_id = ?",
                    (letterboxdAvgNew, timestamp, tmdbId),
                )
        except Exception as e:
            logger.warning(
                "Failed to update letterboxd average for '%s': %s" % (title, e)
            )
            continue
    logger.info("Updated all letterboxd average ratings.")


def fetch_movie_tmdb_ids(db: helper.DB):
    logger.debug("Starting to fetch tmdb IDs...")
    movie_list = []
    with db.ops() as c:
        c.execute(
            "SELECT title, url, tmdb_id, rewatch FROM movies WHERE tmdb_id is 0 or tmdb_id is NULL"
        )
        movie_list = c.fetchall()

    for movie in movie_list:
        title = movie[0]
        url = movie[1]
        tmdbId = movie[2]
        rewatch = movie[3]
        fullUrl = ""
        letterboxdAvg = 0

        logger.info("Parsing '%s' with url '%s'" % (title, url))

        try:
            # Get fullUrl out of review url to save one webrequest
            urlList = url.split("/")
            # Remove empty fields from list
            urlList = list(filter(None, urlList))
            # If rewatch, a number is added to the url
            if rewatch:
                fullUrl = "https://letterboxd.com/film/" + urlList[-2]
            else:
                fullUrl = "https://letterboxd.com/film/" + urlList[-1]
            logger.debug("Using fullUrl: '%s'" % fullUrl)
            soup = BeautifulSoup(
                requests.get(fullUrl, headers=headers).content,
                "html.parser",
            )
            # TMDB from META Tag
            tmdbId = soup.find("body").attrs["data-tmdb-id"]
            letterboxdAvg = fetch_letterboxd_avg(soup, fullUrl)
        except Exception as e:
            logger.warning(
                "Were not able to webrequest meta infos for '%s': %s" % (title, e)
            )

        # On success write meta infos to database
        timestamp = datetime.now().isoformat()
        with db.ops() as c:
            c.execute(
                "UPDATE movies SET tmdb_id = ? WHERE url = ?",
                (tmdbId, url),
            )
        tmdb = helper.TMDB(
            tmdb_id=tmdbId,
            db=db,
            title=title,
            letterboxd_avg=letterboxdAvg,
            letterboxd_avg_date=timestamp,
        )
        logger.info(
            "Set id '%s' and rating '%s' for '%s'"
            % (tmdb.tmdb_id, letterboxdAvg, title)
        )
    logger.debug("Fetched all missing tmdb IDs ...")


def fetch_movies(user_list, db):
    """
    Collects movies from user's rss feed and saves them to the database

    :param user_list:
    :param db:
    :return:
    """
    for user in user_list:
        logger.debug(f"Fetching movies for user '%s' ..." % user_list[user].username)
        try:
            feed = feedparser.parse(user_list[user].feed_url)
            for e in feed.entries:
                try:
                    if "/list/" in e.link:
                        # No need to parse a movie list
                        logger.debug("Skipping entry, contains unparseable list.")
                        continue
                    if hasattr(e, "letterboxd_memberrating"):
                        # e.letterboxd_memberrating not empty
                        movie = helper.Movie(
                            letterboxd_id=e.id,
                            db=db,
                            url=e.link,
                            title=e.letterboxd_filmtitle,
                            year=int(e.letterboxd_filmyear),
                            rating=e.letterboxd_memberrating,
                            date=datetime.fromtimestamp(
                                mktime(e.published_parsed)
                            ).isoformat(),
                            user=user_list[user],
                            rewatch=int(e.letterboxd_rewatch == "Yes"),
                        )
                        logger.debug(f"Saved movie '%s' to database ..." % movie.title)
                    else:
                        movie = helper.Movie(
                            letterboxd_id=e.id,
                            db=db,
                            url=e.link,
                            title=e.letterboxd_filmtitle,
                            year=int(e.letterboxd_filmyear),
                            rating=0,  # Users cannot rate 0 on letterboxd, so we can use it
                            date=datetime.fromtimestamp(
                                mktime(e.published_parsed)
                            ).isoformat(),
                            user=user_list[user],
                            rewatch=int(e.letterboxd_rewatch == "Yes"),
                        )
                        logger.debug(f"Saved movie '%s' to database ..." % movie.title)
                except BaseException as err:
                    logger.debug(err)
                    logger.debug("Error while trying to parse movie. Continuing ...")
                    continue
        except BaseException as err:
            logger.debug(err)
            logger.debug(
                f"Error while catching feed for user '%s'. Skipping ..."
                % user_list[user].username
            )
            continue
