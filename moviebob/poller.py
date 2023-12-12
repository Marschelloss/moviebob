import feedparser
from logzero import logger
from moviebob import helper
from datetime import datetime
from time import mktime


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
            parsed_user = helper.User(username=parsed_string[0], nickname=parsed_string[1], db=db)
        else:
            logger.error(f"Could not parse provided user string '%s'. Need format 'username:nickname'! Exiting ..." %
                         user)
            exit(1)
        parsed_user_list[parsed_user.username] = parsed_user

    return parsed_user_list


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
                    if hasattr(e, "letterboxd_memberrating"):
                        # e.letterboxd_memberrating not empty
                        movie = helper.Movie(
                            letterboxd_id=e.id,
                            db=db,
                            url=e.link,
                            title=e.letterboxd_filmtitle,
                            year=int(e.letterboxd_filmyear),
                            rating=e.letterboxd_memberrating,
                            date=datetime.fromtimestamp(mktime(e.published_parsed)).isoformat(),
                            user=user_list[user],
                            rewatch=int(e.letterboxd_rewatch == 'Yes')
                        )
                        logger.debug(f"Saved movie '%s' to database ..." % movie.title)
                    else:
                        movie = helper.Movie(
                            letterboxd_id=e.id,
                            db=db,
                            url=e.link,
                            title=e.letterboxd_filmtitle,
                            year=int(e.letterboxd_filmyear),
                            rating=0, # Users cannot rate 0 on letterboxd, so we can use it
                            date=datetime.fromtimestamp(mktime(e.published_parsed)).isoformat(),
                            user=user_list[user],
                            rewatch=int(e.letterboxd_rewatch == 'Yes')
                        )
                        logger.debug(f"Saved movie '%s' to database ..." % movie.title)
                except BaseException as err:
                    logger.debug(err)
                    logger.debug("Error while trying to parse movie. Continuing ...")
                    continue
        except BaseException as err:
            logger.debug(err)
            logger.debug(f"Error while catching feed for user '%s'. Skipping ..." % user_list[user].username)
            continue
