#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
from telegram import Bot
from logzero import logger, loglevel
from moviebob import helper
from moviebob import poller
from moviebob import telegram


def main(args):
    if args.verbose > 0:
        loglevel(level=10)
    else:
        loglevel(level=20)

    # Print arguments
    logger.debug(f"Arguments: %s" % args)
    # Starting Bot
    bot = Bot(args.telegram_bot_token)
    # Setup DB
    db = helper.DB(args.database)

    if args.letterboxd_user is None:
        logger.info("No Letterboxd users provided - nothing to fetch. Exiting ...")
        exit(1)

    user_list = poller.setup_users(args.letterboxd_user, db)
    poller.fetch_movies(user_list, db)
    telegram.send_movie_updates(db, bot, args.telegram_chat_id, user_list)
    telegram.fetch_monthly_update(db, bot, args.telegram_chat_id, user_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Telegram Chat ID
    parser.add_argument(
        "-i",
        "--telegram_chat_id",
        action="store",
        required=True,
        help="Telegram chat ID to report to",
    )

    # Telegram Bot Token
    parser.add_argument(
        "-t",
        "--telegram_bot_token",
        action="store",
        required=True,
        help="Telegram Bot token to send messages",
    )

    # Letterboxd User List
    parser.add_argument(
        "-l", "--letterboxd-user", action="append", help="Letterboxd username to watch. Custom notification nickname "
                                                         "can be provided via colon e.g. `username:nickname`"
    )

    # SQLite Database Location
    parser.add_argument(
        "-d",
        "--database",
        action="store",
        default="./moviebob.db",
        help="Location of SQLite Database file. Defaults to `./moviebob.db`"
    )

    # Optional verbosity counter (eg. -v, -vv, -vvv, etc.)
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity (-v, -vv, etc)")

    # Specify output of "--version"
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    main(args)
