from time import sleep
from logzero import logger
from datetime import datetime
from dateutil import relativedelta
import telegram


def send_movie_updates(db, bot, chat_id, user_list):
    """
    Sends updates about new movies to specific telegram group
    :param db:
    :param bot:
    :param chat_id:
    :param user_list:
    :return:
    """
    with db.ops() as c:
        c.execute(
            """
            SELECT title, url, user, movie_id, tmdb_id, rewatch FROM movies WHERE notified = 0;
        """
        )
        r = c.fetchall()
        for movie in r:
            for user in user_list:
                if user_list[user].user_id == movie[2]:
                    msg_text = ""
                    c.execute(
                        """SELECT
                                tmdb_id,
                                title,
                                runtime,
                                release_date,
                                letterboxd_avg,
                                shortfilm
                            FROM tmdb
                            WHERE tmdb_id = ?""",
                        (movie[4],),
                    )
                    meta = c.fetchone()
                    runtime = meta[2]
                    letterboxd_avg = meta[4]
                    if letterboxd_avg == float("0.0"):
                        letterboxd_avg = ""
                    icon = "🍿"
                    shortfilm = meta[5]
                    if shortfilm and movie[5]:
                        icon = "🍿🩳🔄"
                    elif shortfilm:
                        icon = "🍿🩳"
                    elif movie[5]:
                        icon = "🍿🔄"
                    if shortfilm and runtime and letterboxd_avg:
                        msg_text = (
                            f"%s %s hat sich '%s' mit %s Minuten Länge (Shortfilm: 0,5 Pkt.) und einer durschnittlichen Letterboxd Wertung von %s/5 reingezogen: %s"
                            % (
                                icon,
                                user_list[user].nickname,
                                movie[0],
                                runtime,
                                letterboxd_avg,
                                movie[1],
                            )
                        )
                    elif runtime and letterboxd_avg:
                        msg_text = (
                            f"%s %s hat sich '%s' mit %s Minuten Länge und einer durschnittlichen Letterboxd Wertung von %s/5 reingezogen: %s"
                            % (
                                icon,
                                user_list[user].nickname,
                                movie[0],
                                runtime,
                                letterboxd_avg,
                                movie[1],
                            )
                        )
                    elif shortfilm and runtime:
                        msg_text = (
                            f"%s %s hat sich '%s' mit %s Minuten Länge (Shortfilm: 0,5 Pkt.) reingezogen: %s"
                            % (
                                icon,
                                user_list[user].nickname,
                                movie[0],
                                runtime,
                                movie[1],
                            )
                        )
                    elif runtime:
                        msg_text = (
                            f"%s %s hat sich '%s' mit %s Minuten Länge reingezogen: %s"
                            % (
                                icon,
                                user_list[user].nickname,
                                movie[0],
                                runtime,
                                movie[1],
                            )
                        )
                    elif letterboxd_avg:
                        msg_text = (
                            f"%s %s hat sich '%s' mit einer durschnittlichen Letterboxd Wertung von %s/5 reingezogen: %s"
                            % (
                                icon,
                                user_list[user].nickname,
                                movie[0],
                                letterboxd_avg,
                                movie[1],
                            )
                        )
                    else:
                        msg_text = f"%s %s hat sich '%s': %s" % (
                            icon,
                            user_list[user].nickname,
                            movie[0],
                            movie[1],
                        )
                    send_movie_msg(bot, chat_id, msg_text, movie[3], db)
    logger.info("Every movie in database got parsed :)")


def send_movie_msg(bot, chat_id, msg, movie_id, db, attempt=0):
    if attempt > 2:
        logger.info(f"Maximum attempts reached. Skipping '%s' ..." % msg)
        return
    attempt = attempt + 1
    try:
        logger.info(f"Attempt %s: Sending Notification: %s" % (attempt, msg))
        bot.sendMessage(
            chat_id=chat_id,
            text=msg,
        )
        sleep(0.3)  # Helps with timeouts
    except telegram.error.TimedOut:
        logger.debug(f"Sending '%s' timed out!" % msg)
        sleep(3)
        send_movie_msg(bot, chat_id, msg, movie_id, db, attempt)
    except telegram.error.RetryAfter as err:
        logger.info(
            f"Sending '%s' was blocked. Retrying in %s seconds ..."
            % (msg, err.retry_after)
        )
        sleep(err.retry_after)
        send_movie_msg(bot, chat_id, msg, movie_id, db, attempt)
    except BaseException as e:
        logger.debug(f"Unknown error while sending telegram message: %s" % e)
        sleep(3)
        send_movie_msg(bot, chat_id, msg, movie_id, db, attempt)
    else:
        # If no exception was thrown
        with db.ops() as c:
            c.execute(
                """
                UPDATE movies
                SET notified = 1
                WHERE movie_id = ?
            """,
                (movie_id,),
            )
            return


def fetch_monthly_update(db, bot, chat_id, user_list):
    """
    Checks if the monthly update got sent out and prepares the message if not
    :param bot:
    :param db:
    :param user_list:
    :return:
    """
    current_month = datetime.now().month
    current_year = datetime.now().year
    logger.debug(f"Checking for monthly update %s-%s" % (current_year, current_month))
    with db.ops() as c:
        c.execute(
            """
            SELECT * FROM monthly WHERE month is ? AND year is ?
        """,
            (current_month, current_year),
        )
        r = c.fetchone()
        if r is None:
            logger.info("Monthly update not sent, preparing message ...")
            send_monthly_msg(
                bot,
                chat_id,
                create_monthly_msg(db, user_list),
                current_month,
                current_year,
                db,
            )


def create_monthly_msg(db, user_list):
    watch_list = {}
    rewatch_list = {}
    msg_list = []
    target_month = (datetime.now() + relativedelta.relativedelta(months=-1)).month
    target_year = (datetime.now() + relativedelta.relativedelta(months=-1)).year
    target_start = f"datetime('%d-%02d-01 00:00:00')" % (target_year, target_month)
    target_end = f"datetime('%d-%02d-31 23:59:59')" % (target_year, target_month)

    for user in user_list:
        username = user_list[user].nickname
        with db.ops() as c:
            c.execute(
                "SELECT title, rewatch FROM movies WHERE user is "
                + str(user_list[user].user_id)
                + " AND date BETWEEN "
                + target_start
                + " AND "
                + target_end
            )
            r = c.fetchall()
            watch_list[username] = len(r)
            rewatch_list[username] = sum(map(lambda x: x[1] == 1, r))

    watch_list = dict(
        sorted(watch_list.items(), key=lambda item: item[1], reverse=True)
    )
    for i, user in enumerate(watch_list):
        if i == 0:
            msg_list.append(
                "- 🥇 Wuhu! Gute Arbeit! %s hat sich massive %s Filme reingedübelt, davon %s Rewatches"
                % (user, watch_list[user], rewatch_list[user])
            )
        elif i == 1:
            msg_list.append(
                "- 🥈 Zweiter Platz für %s! Hat sich ordentlich %s Filme einverleibt, davon %s Rewatches"
                % (user, watch_list[user], rewatch_list[user])
            )
        elif i == 2:
            msg_list.append(
                "- 🥉 Letztes Edelmetal geht an %s mit %s Filmen unterm Gürtel, davon %s Rewatches"
                % (user, watch_list[user], rewatch_list[user])
            )
        else:
            msg_list.append(
                "- 🍑 %s hatte wohl Bessers zu tun, und schaffte es nur auf %s Film(e), "
                "davon %s Rewatche(s)" % (user, watch_list[user], rewatch_list[user])
            )

    msg_header = (
        "🎬 Endlich ist es wieder so weit - Zeit für den monatlichen Penisvergleich! Die Stats für %s-%s:\n\n"
        % (target_month, target_year)
    )
    msg = msg_header + "\n\n".join(msg_list)
    return msg


def send_monthly_msg(bot, chat_id, msg, current_month, current_year, db, attempt=0):
    if attempt > 2:
        logger.debug(f"Maximum attempts reached. Skipping '%s' ..." % msg)
        return
    attempt = attempt + 1
    try:
        logger.info(f"Attempt %s: Sending Notification: %s" % (attempt, msg))
        bot.sendMessage(
            chat_id=chat_id,
            text=msg,
        )
        sleep(0.3)  # Helps with timeouts
    except telegram.error.TimedOut:
        logger.debug(f"Sending '%s' timed out!" % msg)
        sleep(3)
        send_monthly_msg(bot, chat_id, msg, current_month, current_year, db, attempt)
    except telegram.error.RetryAfter as err:
        logger.info(
            f"Sending '%s' was blocked. Retrying in %s seconds ..."
            % (msg, err.retry_after)
        )
        sleep(err.retry_after)
        send_monthly_msg(bot, chat_id, msg, current_month, current_year, db, attempt)
    except BaseException as e:
        logger.debug(f"Unknown error while sending telegram message: %s" % e)
        sleep(3)
        send_monthly_msg(bot, chat_id, msg, current_month, current_year, db, attempt)
    else:
        # If no exception was thrown
        with db.ops() as c:
            c.execute(
                """
                INSERT into monthly(month, year, notified)
                VALUES (?, ?, ?)
            """,
                (current_month, current_year, 1),
            )
            return
