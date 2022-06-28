from datetime import datetime
from logging import getLogger

import sqlalchemy
from telegram import Bot

import misc.config as config
from app.run import create_at_job, get_offset_date
from misc.constants import ENABLED_NEWS_NOTIFICATION

logger = getLogger('make_tasks')
engine = sqlalchemy.create_engine(config.db_connection_string)

try:
    bot = Bot(config.bot_token)
    users_to_add = engine.execute(
        sqlalchemy.text(
            "SELECT * FROM users.usergroup "
            "WHERE "
            "(send_msg_time IS NOT NULL AND job_id IS NULL) "
            "OR "
            "(send_news_time IS NOT  NULL AND news_job_id IS NULL)"
        )
    )
    for row in users_to_add:
        try:
            if row['send_msg_time'] is not None:
                job_id = create_at_job(
                    row['user_id'], row['send_msg_time'].strftime('%H:%M'))
                with engine.connect() as conn:
                    result = conn.execute(
                        sqlalchemy.text(
                            "UPDATE users.usergroup "
                            "SET job_id = :jid "
                            "WHERE user_id = :uid"
                        ),
                        uid=row['user_id'],
                        jid=job_id
                    )
            elif row['send_news_time'] is not None:
                job_id = create_at_job(
                    row['user_id'],
                    row['send_news_time'].strftime('%H:%M'),
                    ENABLED_NEWS_NOTIFICATION
                )
                with engine.connect() as conn:
                    result = conn.execute(
                        sqlalchemy.text(
                            "UPDATE users.usergroup "
                            "SET news_job_id = :jid "
                            "WHERE user_id = :uid"
                        ),
                        uid=row['user_id'],
                        jid=job_id
                    )
        except Exception as e:
            logger.error(e, exc_info=True)
            pass

    users_to_add = engine.execute(
        sqlalchemy.text(
            "SELECT * FROM users.usergroup "
            "WHERE offset_time IS NOT NULL "
            "AND job_id IS NULL"
        )
    )

    for row in users_to_add:
        try:
            date_string = get_offset_date(
                user_id=row['user_id'],
                input_time=datetime.combine(datetime.min, row['offset_time'])
            )

            job_id = create_at_job(row['user_id'], date_string)

            with engine.connect() as conn:
                result = conn.execute(
                    sqlalchemy.text(
                        "UPDATE users.usergroup "
                        "SET job_id = :jid "
                        "WHERE user_id = :uid"
                    ),
                    uid=row['user_id'],
                    jid=job_id
                )
        except Exception as e:
            logger.error(e, exc_info=True)

except Exception as e:
    logger.error(e, exc_info=True)
