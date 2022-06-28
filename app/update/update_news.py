import datetime

import requests
import sqlalchemy
from telegram import Bot
from logging import getLogger
from app.get_user_token import get_user_token
from misc.config import (bot_token, db_connection_string, nstu_login,
                         nstu_password)
from app.run import get_news_from_db

logger = getLogger('update_news')


def send_new_news(news_count):
    with engine.begin() as conn:
        rows = conn.execute(sqlalchemy.text(
            '''
            SELECT user_id
            FROM users.usergroup
            WHERE send_news_immediately IS true
            '''
            )
        )
        bot = Bot(bot_token)
        news = get_news_from_db(news_count)
        for row in rows:
            bot.send_message(
                row['user_id'],
                news,
                parse_mode='HTML',
                disable_web_page_preview=True
            )


current_date = datetime.date.today().strftime('%Y/%m/%d')

engine = sqlalchemy.create_engine(db_connection_string)
jsonnews = requests.get(
    url='https://api.ciu.nstu.ru/v1.0/news/schoolkids/' + current_date,
    cookies={'NstuSsoToken': get_user_token(nstu_login, nstu_password)}
)
try:
    with engine.begin() as conn:
        rows_count = conn.execute(sqlalchemy.text(
            '''
                DELETE FROM test.json_news;
                INSERT INTO test.json_news (data) VALUES (:vl);
                INSERT INTO test.news
                    SELECT * FROM test.fill_news_view
                    ON CONFLICT (id) DO NOTHING
            '''
            ),
            vl=jsonnews.text
        )
    if rows_count.rowcount > 0:
        send_new_news(rows_count.rowcount)
except Exception as e:
    logger.error(e, exc_info=True)
    with engine.begin() as conn:
        file = open('../misc/sql/create/news.sql')
        conn.execute(sqlalchemy.text(file.read()))
        conn.execute(sqlalchemy.text(
            '''
            DELETE FROM test.json_news;
            INSERT INTO test.json_news (data) VALUES (:vl);
            INSERT INTO test.news
                SELECT *
                FROM test.fill_news_view
                ON CONFLICT (id) DO NOTHING
            '''),
            vl=jsonnews.text
        )

logger.info(f'done {str(rows_count.rowcount)} news')
