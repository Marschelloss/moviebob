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


def fetch_monthly_update(db, bot, chat_id):
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
                create_monthly_msg(db),
                current_month,
                current_year,
                db,
            )


def create_monthly_msg(db):
    watch_list = []
    rewatch_list = []
    shortfilm_list = []
    msg_list = []
    user_list = []
    target_month = (datetime.now() + relativedelta.relativedelta(months=-1)).month
    target_year = (datetime.now() + relativedelta.relativedelta(months=-1)).year
    target_start = f"datetime('%d-%02d-01 00:00:00')" % (target_year, target_month)
    target_end = f"datetime('%d-%02d-31 23:59:59')" % (target_year, target_month)

    with db.ops() as c:
        c.execute(
            """
            SELECT user, nickname, COUNT(movie_id)
            FROM movies
            INNER JOIN users ON users.user_id = movies.user
            WHERE date BETWEEN %s AND %s
            GROUP BY user
            ORDER BY COUNT(movie_id) DESC
            """
            % (target_start, target_end)
        )
        watch_list = c.fetchall()
        c.execute(
            """
            SELECT user, nickname, COUNT(movie_id)
            FROM movies
            INNER JOIN users ON users.user_id = movies.user
            WHERE rewatch = 1 AND date BETWEEN %s AND %s
            GROUP BY user
            ORDER BY COUNT(movie_id) DESC
            """
            % (target_start, target_end)
        )
        rewatch_list = c.fetchall()
        c.execute(
            """
            SELECT user, nickname, COUNT(movie_id)
            FROM movies
            INNER JOIN users ON users.user_id = movies.user
            INNER JOIN tmdb ON tmdb.tmdb_id = movies.tmdb_id
            WHERE shortfilm = 1 AND date BETWEEN %s AND %s
            GROUP BY user
            ORDER BY COUNT(movie_id) DESC
            """
            % (target_start, target_end)
        )
        shortfilm_list = c.fetchall()

    for user in watch_list:
        user_id = user[0]
        nickname = user[1]
        watch_count = user[2]
        rewatch_count = 0
        for rewatch in rewatch_list:
            if rewatch[0] == user_id:
                rewatch_count = rewatch[2]
        shortfilm_count = 0
        for shortfilm in shortfilm_list:
            if shortfilm[0] == user_id:
                shortfilm_count = shortfilm[2]

        # Probably should be reworked into the User Class
        user_list.append(
            {
                "user_id": user_id,
                "nickname": nickname,
                "watch_count": watch_count,
                "rewatch_count": rewatch_count,
                "shortfilm_count": shortfilm_count,
            }
        )

    for i, user in enumerate(user_list):
        if i == 0:
            msg_list.append(
                "- 🥇 Wuhu! Gute Arbeit! %s hat sich massive %s Filme reingedübelt, davon %s Rewatches und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )
        elif i == 1:
            msg_list.append(
                "- 🥈 Zweiter Platz für %s! Hat sich ordentlich %s Filme einverleibt, davon %s Rewatches und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )
        elif i == 2:
            msg_list.append(
                "- 🥉 Letztes Edelmetal geht an %s mit %s Filmen unterm Gürtel, davon %s Rewatches und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )
        elif i == 3:
            msg_list.append(
                "- 🍄 Knapp am Podium vorbei! %s hat sich trotzdem %s Filme reingedübelt, davon %s Rewatches und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )
        elif i == 4:
            msg_list.append(
                "- 🥑 Schon wenig, aber immernoch besser als Letzer! %s hat sich %s Filme gegönnt, davon %s Rewatches und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )
        else:
            msg_list.append(
                "- 🍑 %s hatte wohl Bessers zu tun, und schaffte es nur auf %s Film(e), "
                "davon %s Rewatche(s) und %s Shortfilms"
                % (
                    user["nickname"],
                    user["watch_count"],
                    user["rewatch_count"],
                    user["shortfilm_count"],
                )
            )

    if not msg_list:
        raise Exception("Error: msg_list is empty!")

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
