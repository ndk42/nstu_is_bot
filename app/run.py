import datetime
import locale
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from html import unescape
from subprocess import call

import pandas
import sqlalchemy
from rapidfuzz import fuzz, process
from telegram import (CallbackQuery, ForceReply, InlineKeyboardButton,
                      InlineKeyboardMarkup, ReplyKeyboardMarkup, Update,
                      error, Message)
from telegram.ext import (CallbackContext, CallbackQueryHandler,
                          CommandHandler, ConversationHandler, Filters,
                          MessageHandler, PicklePersistence, Updater)
from transliterate import translit

import misc.config as config
import misc.constants as cns


@dataclass
class NotifyMode:
    schedule: str
    news: str


menu_keyboard_markup = ReplyKeyboardMarkup(
    cns.cns.MENU_BUTTONS,
    one_time_keyboard=False,
    resize_keyboard=True
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)

# for datetime format
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

logger = logging.getLogger(__name__)

days_dict = {1: 'Пн', 2: 'Вт', 3: 'Ср', 4: 'Чт', 5: 'Пт', 6: 'Сб', 7: 'Вс'}

engine = sqlalchemy.create_engine(config.db_connection_string)

settings_state_dict = {}

# Сообщение, которое нам нужно удалить что бы в чатике было красиво.
last_unused_messages_dict = {}


def TIMETABLE_ROW_TEMPLATE(row) -> str:
    return (
        f"[{row['pair_number']}] {row['starttime']}-{row['endtime']} "
        f"{'[' + row['tsw_name'] + '] ' if row['tsw_name'] is not None else ''} "
        f"{row['classname']} {row['rooms'] if row['rooms'] is not None else ''} "
        f"{row['teacher1']} {row['teacher2']}\n"
    )


def start(update: Update, context: CallbackContext):
    context.dispatcher.run_async(
        update.message.reply_text,
        text='Привет. Я бот-помощник студента НГТУ \n'
             'Отправьте /cancel если хотите прервать общение.\n\n'
             'Введите вашу группу',
        reply_markup=ForceReply(),
        update=update
    )

    return cns.cns.CLAIM_USER_GROUP_HANDLER


def timetable_markup(chosen_time: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(
            "Расписание на текущий день "
            f"{'✅' if chosen_time == cns.cns.DAY_SCHEDULE else ''}",
            callback_data=cns.cns.DAY_SCHEDULE
        )],
        [InlineKeyboardButton(
            "Расписание на оставшуюся неделю "
            f"{'✅' if chosen_time == cns.cns.WEEK_SCHEDULE else ''}",
            callback_data=cns.cns.WEEK_SCHEDULE
        )],
        [InlineKeyboardButton(
            "Расписание на выбранную неделю "
            f"{'✅' if chosen_time == cns.cns.SPECIFIC_WEEK_SCHEDULE else ''}",
            callback_data=cns.cns.SPECIFIC_WEEK_SCHEDULE
        )]]
    return InlineKeyboardMarkup(keyboard)


def news_markup(chosen_news_interval: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(
            "Новости на текущий день "
            f"{'✅' if chosen_news_interval == cns.DAY_NEWS else ''}",
            callback_data=cns.DAY_NEWS
        )],
        [InlineKeyboardButton(
            "Последние 5 новостей "
            f"{'✅' if chosen_news_interval == cns.LAST_FIVE_NEWS else ''}",
            callback_data=cns.LAST_FIVE_NEWS
        )],
        [InlineKeyboardButton(
            "Новости на выбранную дату "
            f"{'✅' if chosen_news_interval == cns.SPECIFIC_DATE_NEWS else ''}",
            callback_data=cns.SPECIFIC_DATE_NEWS
        )]
    ]
    return InlineKeyboardMarkup(keyboard)


def settings_markup(notification_mode: NotifyMode) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(
            'Подписаться на ежедневную отправку расписания'
            if notification_mode.schedule == cns.DISABLED_SCHEDULE_NOTIFICATION
            else 'Оптисаться от ежедневной отправки расписания',
            callback_data=notification_mode.schedule
        )],
        [InlineKeyboardButton(
            'Подписаться на новости'
            if notification_mode.news == cns.DISABLED_NEWS_NOTIFICATION
            else 'Отписаться от новостей',
            callback_data=notification_mode.news
        )]
    ]
    return InlineKeyboardMarkup(keyboard)


def schedule_notification_settings_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(
            "Выбрать время отправки расписания",
            callback_data=cns.SPECIFY_SEND_MSG_TIME
        )],
        [InlineKeyboardButton(
            "Выбрать времени оповещения относительно первой пары",
            callback_data=cns.cns.SPECIFY_SEND_MSG_TIME_OFFSET
        )],
        [InlineKeyboardButton(
            "Назад",
            callback_data=cns.BACK_TO_SETTINGS
        )]
    ]
    return InlineKeyboardMarkup(keyboard)


def news_notification_settings_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(
            "Выбрать время отправки расписания",
            callback_data=cns.SPECIFY_SEND_NEWS_TIME
        )],
        [InlineKeyboardButton(
            "Немедленная отправка расписания",
            callback_data=cns.SEND_NEWS_IMMEDIATELY
        )],
        [InlineKeyboardButton(
            "Назад",
            callback_data=cns.BACK_TO_SETTINGS
        )]
    ]
    return InlineKeyboardMarkup(keyboard)


def weeks_num_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("1", callback_data='WEEK1'),
            InlineKeyboardButton("2", callback_data='WEEK2'),
            InlineKeyboardButton("3", callback_data='WEEK3')
        ],
        [
            InlineKeyboardButton("4", callback_data='WEEK4'),
            InlineKeyboardButton("5", callback_data='WEEK5'),
            InlineKeyboardButton("6", callback_data='WEEK6')
        ],
        [
            InlineKeyboardButton("7", callback_data='WEEK7'),
            InlineKeyboardButton("8", callback_data='WEEK8'),
            InlineKeyboardButton("9", callback_data='WEEK9')
        ],
        [
            InlineKeyboardButton("10", callback_data='WEEK10'),
            InlineKeyboardButton("11", callback_data='WEEK11'),
            InlineKeyboardButton("12", callback_data='WEEK12')
        ],
        [
            InlineKeyboardButton("13", callback_data='WEEK13'),
            InlineKeyboardButton("14", callback_data='WEEK14'),
            InlineKeyboardButton("15", callback_data='WEEK15')
        ],
        [
            InlineKeyboardButton("16", callback_data='WEEK16'),
            InlineKeyboardButton("17", callback_data='WEEK17'),
            InlineKeyboardButton("18", callback_data='WEEK18')
        ],
        [
            InlineKeyboardButton("Назад", callback_data=cns.DAY_SCHEDULE)
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# Built in pickle can't save states in async handlers,
# so i made asynchronous only functions, that interract with
# database and telegram server


def edit_message_text_and_markup_async(query, message_args, markup_args):
    try:
        query.edit_message_text(**message_args)
        query.edit_message_reply_markup(**markup_args)
    except error.BadRequest:
        # Catch this exception when we delete inline keyboard
        pass


def reply_and_delete_message_async(message: Message, message_args) -> None:
    message.delete()
    message.reply_text(**message_args)


def get_true_groups_name(input: str, group_names: list) -> list:
    return process.extract(
        translit(input.upper(), 'ru'),
        group_names,
        scorer=fuzz.partial_ratio,
        limit=5
    )
    # return list(map(list, zip(*score)))


def get_first_study_day_date() -> datetime.date:
    if datetime.date.today().month < 8 and datetime.date.today().month > 2:
        return datetime.date(datetime.date.today().year, 9, 1)
    else:
        return datetime.date(datetime.date.today().year, 9, 1)


def get_days_by_week(week_to_check: int) -> list:
    given_week_rnd_day = get_first_study_day_date(
    ) + datetime.timedelta(weeks=week_to_check - 1)
    dates = [given_week_rnd_day + datetime.timedelta(days=i) for i in range(
        0 - given_week_rnd_day.weekday(), 7 - given_week_rnd_day.weekday())]
    # days = [datetime.datetime.strptime(str(year) + "-" + str(week - 1) 
    # + "-" + str(x), "%Y-%W-%u") for x in range(1, 8)]
    dates_strings = [datetime.date.strftime(
        day, '%a. %d %B %Y') for day in dates]
    return dates_strings


def get_current_week() -> int:
    return datetime.date.today().isocalendar()[1] \
        - get_first_study_day_date().isocalendar()[1] + 1


# the name of this function is nod to history of creating this bot
def init_user(update: Update, context: CallbackContext):
    try:
        with engine.begin() as conn:
            group_names_query = conn.execute(
                sqlalchemy.text("SELECT name FROM test.group_names"))
            group_names = [row['name'] for row in group_names_query]

            true_group = get_true_groups_name(update.message.text, group_names)
            keyboard = []
            for group, _ in true_group:
                if _ == 100.0:
                    with engine.begin() as conn:
                        conn.execute(sqlalchemy.text(
                            "INSERT INTO users.usergroup (user_id, group_name) "
                            "VALUES (:u_id, :gn) ON CONFLICT (user_id) DO UPDATE "
                            "SET group_name = :gn"),
                            u_id=update.message.from_user.id,
                            gn=group
                            )
                    # We resend message with markup,
                    # because callback_query can't send menu keyboard as markup
                    update.message.reply_text(
                        text=f'Ваша группа {group}!\nПоздравляю вас\n',
                        reply_markup=menu_keyboard_markup
                    )
                    return ConversationHandler.END
                keyboard.append(
                    [InlineKeyboardButton(group, callback_data=group)])
            keyboard.append([InlineKeyboardButton(
                'Другая группа', callback_data='Другая группа')])
            context.dispatcher.run_async(
                update.message.reply_text,
                text='Выберите группу',
                reply_markup=InlineKeyboardMarkup(keyboard),
                update=update
            )
            return cns.SET_USER_GROUP_HANDLER

    except Exception as e:
        logger.error(str(e), exc_info=True)


# todo: delete old notification setting if user changes his group
def select_group(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    if query.data == 'Другая группа':
        query.message.delete()
        change_user_group(query, context)
        return cns.CLAIM_USER_GROUP_HANDLER
    try:
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(
                "INSERT INTO users.usergroup (user_id, group_name) " +
                "VALUES (:u_id, :gn) " +
                "ON CONFLICT (user_id) DO UPDATE " +
                "SET group_name = :gn"),
                u_id=query.from_user.id,
                gn=query.data
            )
        # We resend message with markup,
        # because callback_query can't send menu keyboard as markup
        context.dispatcher.run_async(
            reply_and_delete_message_async,
            query.message,
            {
                'text': f'Ваша группа {update.callback_query.data}!\n' +
                'Поздравляю вас\n',
                'reply_markup': menu_keyboard_markup
            },
            update=update
        )
    except sqlalchemy.exc.IntegrityError:   # Ignored, becasue of INSERT ON CONFLICT
        context.dispatcher.run_async(
            reply_and_delete_message_async,
            query.message,
            {
                'text': 'Вы уже есть тут, шо вам еще надо?',
                'reply_markup': menu_keyboard_markup
            },
            update=update
        )
    fallback(update, context)
    return ConversationHandler.END


def change_user_group(update: Update, context: CallbackContext):
    update.message.reply_text(
        text='Введите имя группы:\n(или фамилию если вы преподаватель)',
        reply_markup=ForceReply()
    )
    return cns.CLAIM_USER_GROUP_HANDLER


def cancel(update: Update, context: CallbackContext):
    update.message.reply_text(text='Не пытайся лезть сюда. Фу')
    return cns.CLAIM_USER_GROUP_HANDLER


def get_user_day_timetable(user_id: int):
    with engine.connect() as conn:
        current_week = get_current_week()
        if current_week < 19:
            week_check_str = 'AND ((is_odd = -1 AND week18 = True) OR (is_odd = 0))'\
                if current_week == 18 \
                else f'AND week{current_week} = true'

            result = conn.execute(sqlalchemy.text(
                "SELECT * "
                f"FROM {cns.TIMETABLE_NAME} "
                "WHERE group_name IN "
                "   (SELECT group_name "
                "   FROM users.usergroup "
                "   WHERE user_id = :uid "
                "   LIMIT 1"
                ") "
                "AND day = (select extract(isodow from now())) "
                f"{week_check_str} "
                "ORDER BY starttime"),
                uid=user_id
            )
            if result.rowcount == 0:
                return None
            else:
                return ''.join(map(TIMETABLE_ROW_TEMPLATE, result))


def get_user_week_timetable(user_id: int, week_to_check, is_rest_week, context_async=None, update_async=None):
    rest_week_sql = f'AND ((day = EXTRACT(isodow from {config.SQL_NOW}) '\
                    f'AND endtime > to_char({config.SQL_NOW}, \'HH24:MI\')) '\
                    f'OR (day>EXTRACT(isodow from {config.SQL_NOW})))'

    week_check_str = 'AND ((is_odd = -1 AND week18 = True) OR (is_odd = 0))'\
        if week_to_check == 18\
        else\
        f'AND week{week_to_check} = true'

    if context_async is None and update_async is None:
        days_of_given_week = get_days_by_week(week_to_check)
        with engine.connect() as conn:
            result = pandas.read_sql(sqlalchemy.text(
                f"SELECT * FROM {cns.TIMETABLE_NAME} "
                "WHERE group_name IN ("
                "   SELECT group_name"
                "   FROM users.usergroup"
                "   WHERE user_id = :uid "
                "   LIMIT 1) "
                f"{week_check_str} "
                f"{rest_week_sql if is_rest_week == True else ''} "
                "ORDER BY day, starttime"),
                conn,
                params={'uid': user_id}
            )
            days_timetable_list = []
            days = result[['day']].groupby('day').count()
            for day_idx in days.index:
                current_day_text = ""
                current_day_timetable = result[result['day'] == int(day_idx)]
                current_day_text += days_of_given_week[int(day_idx) - 1] + '\n'
                for _index, row in current_day_timetable.iterrows():
                    current_day_text += TIMETABLE_ROW_TEMPLATE(row)
                days_timetable_list.append(current_day_text)
        return days_timetable_list
    else:
        days_of_given_week = context_async.dispatcher.run_async(
            get_days_by_week,
            week_to_check,
            update=update_async
        )
        with engine.connect() as conn:
            result = pandas.read_sql(sqlalchemy.text(
                f"SELECT * FROM {cns.TIMETABLE_NAME} "
                "WHERE group_name IN ("
                "   SELECT group_name "
                "   FROM users.usergroup "
                "   WHERE user_id = :uid "
                "   LIMIT 1) "
                f"{week_check_str} "
                f"{rest_week_sql if is_rest_week == True else ''} "
                "ORDER BY day, starttime"),
                conn,
                params={'uid': user_id}
            )
        days_timetable_list = []
        days = result[['day']].groupby('day').count()
        for day_idx in days.index:
            current_day_text = ""
            current_day_timetable = result[result['day'] == int(day_idx)]
            current_day_text += days_of_given_week.result()[
                int(day_idx) - 1] + '\n'
            for _index, row in current_day_timetable.iterrows():
                current_day_text += TIMETABLE_ROW_TEMPLATE(row)
            days_timetable_list.append(current_day_text)
        return days_timetable_list


def proceed_timetable(update: Update, context: CallbackContext) -> str:
    user_timetable = get_user_day_timetable(update.message.from_user.id)
    update.message.reply_text(
        text=f"Сейчас {get_current_week()} неделя\n\n" +
        user_timetable if user_timetable is not None else cns.USER_FREE_DAY,
        reply_markup=timetable_markup(cns.DAY_SCHEDULE)
    )
    return cns.SCHEDULE_MENU_HANDLER


def get_news_from_db(news_interval: str, date: datetime.date = None) -> str:
    news_text = ''
    with engine.begin() as conn:
        news_query = conn.execute(
            sqlalchemy.text(
                'SELECT title, url, shorttext, news_date '
                'FROM test.news '
                + (' WHERE EXTRACT(DAY FROM news_date) = EXTRACT(DAY FROM now()) '
                    if news_interval == cns.DAY_NEWS
                    else f'WHERE DATE(news_date) = \'{date}\' '
                    if news_interval == cns.SPECIFIC_DATE_NEWS
                    else ''
                   )
                + ' ORDER BY news_date DESC '
                + (' LIMIT 5'
                    if news_interval == cns.LAST_FIVE_NEWS
                    else ('LIMIT ' + str(news_interval))
                    if isinstance(news_interval, int)
                    else '')
            )
        )
        for row in news_query:
            news_text += f"{row['title']}\n" \
                         + (('[' + remove_html_tags(unescape(row['shorttext'])) + ']\n')
                            if row['shorttext'] is not None
                            else ''
                            ) \
                         + row['url'] + '\n' + row['news_date'].strftime('%c') + '\n\n'
    return news_text


def proceed_news(update: Update, context: CallbackContext) -> str:
    update.message.reply_text(
        text=get_news_from_db(cns.LAST_FIVE_NEWS),
        reply_markup=news_markup(cns.LAST_FIVE_NEWS),
        parse_mode='HTML',
        disable_web_page_preview=True
    )
    return cns.NEWS_MENU_HANDLER


def news_button_switch(update: Update, context: CallbackContext) -> str:
    query = update.callback_query
    chosen_news_interval = query.data
    query.answer()

    if chosen_news_interval == cns.LAST_FIVE_NEWS or chosen_news_interval == cns.DAY_NEWS:
        news_text_task = context.dispatcher.run_async(
            get_news_from_db,
            chosen_news_interval,
            update=update
        )
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': 'А новостей-то нету :(' if not news_text_task.result(
            ) else news_text_task.result(), 'parse_mode': 'HTML', 'disable_web_page_preview': True},
            {'reply_markup': news_markup(chosen_news_interval)},
            update=update
        )
    elif chosen_news_interval == cns.SPECIFIC_DATE_NEWS:
        query.edit_message_text(
            text='Введите желаемую дату в формате:\nDD.MM.YYYY')
        query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Назад", callback_data=cns.LAST_FIVE_NEWS)]]
            )
        )
        last_unused_messages_dict[query.from_user.id] = query.message.message_id
        return cns.SPECIFIC_DATE_NEWS_HANDLER
    return cns.NEWS_MENU_HANDLER


def proceed_date_news(update: Update, context: CallbackContext) -> None:
    try:
        user_date = datetime.datetime.strptime(
            update.message.text, '%d.%m.%Y').date()
        news_text_task = context.dispatcher.run_async(
            get_news_from_db, cns.SPECIFIC_DATE_NEWS, update=update, date=user_date)
        global last_unused_messages_dict

        if update.message.chat_id in last_unused_messages_dict:     # To avoid crash after restart of app :)
            update.message.bot.delete_message(
                update.message.chat_id, last_unused_messages_dict[update.message.from_user.id])
        context.dispatcher.run_async(
            reply_and_delete_message_async,
            update.message,
            {
                'text': news_text_task.result() if news_text_task.result() else cns.EMPTY_NEWS,
                'reply_markup': news_markup(cns.SPECIFIC_DATE_NEWS),
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            },
            update=update
        )

    except ValueError:
        if update.message.text in cns.PLAIN_MENU_BUTTONS:
            fallback(update, context)
            return ConversationHandler.END
        update.message.delete()
        # fallback(update, context)
        return cns.SPECIFIC_DATE_NEWS_HANDLER


def get_user_notify_mode(user_id: int) -> NotifyMode:
    global settings_state_dict
    if user_id not in settings_state_dict:
        with engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(
                    "SELECT send_msg_time, offset_time, send_news_time, send_news_immediately \
                    FROM users.usergroup WHERE user_id = :uid"
                ),
                uid=user_id
            )
            if result.rowcount == 0:
                # Old = None, user doesnt exists
                return NotifyMode(cns.DISABLED_SCHEDULE_NOTIFICATION, cns.DISABLED_NEWS_NOTIFICATION)
            else:
                tmp = result.fetchone()
                send_msg_time = tmp['send_msg_time']
                send_news_immed = tmp['send_news_immediately']
                offset_time = tmp['offset_time']
                send_news_time = tmp['send_news_time']
                if ((send_msg_time is None) or (not send_msg_time)) and ((offset_time is None) or (not offset_time)):
                    user_schedule_status = cns.DISABLED_SCHEDULE_NOTIFICATION
                else:
                    user_schedule_status = cns.ENABLED_SCHEDULE_NOTIFICATION
                if send_news_immed is False and send_news_time is None:
                    user_news_status = cns.DISABLED_NEWS_NOTIFICATION
                else:
                    user_news_status = cns.ENABLED_NEWS_NOTIFICATION
                # user_news_status = cns.DISABLED_NEWS_NOTIFICATION if send_news_time is None else
                # cns.ENABLED_NEWS_NOTIFICATION

                user_notify_status = NotifyMode(
                    user_schedule_status, user_news_status)
                settings_state_dict[user_id] = user_notify_status
                return user_notify_status
    else:
        return settings_state_dict[user_id]


def proceed_settings_start(update: Update, context: CallbackContext) -> str:
    context.dispatcher.run_async(
        update.message.reply_text,
        text='Настройки подписок',
        # IN THE FUTETRE, CHANGE NUM IN MARKUP AS SELECT FROM DB GET USER RECIEVING MESSAGES MODE
        reply_markup=settings_markup(
            get_user_notify_mode(update.message.from_user.id)),
        update=update
    )
    return cns.START_SETTINGS_HANDLER


def db_set_specific_time_schedule_settings_async(
        engine: sqlalchemy.engine.Engine,
        time,
        user_id,
        update_async, 
        context_async) -> sqlalchemy.engine.CursorResult:
    with engine.begin() as conn:
        job_id = context_async.dispatcher.run_async(
            create_at_job, user_id, time, mode=cns.ENABLED_SCHEDULE_NOTIFICATION, update=update_async)
        user_time = datetime.datetime.strptime(time, '%H:%M')
        group_names_query = conn.execute(sqlalchemy.text(
            "SELECT job_id FROM users.usergroup WHERE user_id=:uid AND job_id IS NOT NULL"), uid=user_id)
        if group_names_query.rowcount != 0:
            old_job_id = group_names_query.fetchone()['job_id']
            call(f'at -r {old_job_id}', shell=True)
        return conn.execute(sqlalchemy.text(
            "UPDATE users.usergroup "
            "SET (send_msg_time, job_id, offset_time)"
            " = (:smt,:jid, NULL) WHERE user_id=:uid"),
            uid=user_id,
            smt=user_time.time(),
            jid=int(job_id.result())
        )


def proceed_schedule_specific_time_settings(update: Update, context: CallbackContext) -> str:
    try:
        context.dispatcher.run_async(
            db_set_specific_time_schedule_settings_async,
            engine,
            update.message.text,
            update.message.from_user.id,
            update=update,
            update_async=update,
            context_async=context
        )
        global settings_state_dict
        settings_state_dict[update.message.from_user.id].schedule = cns.ENABLED_SCHEDULE_NOTIFICATION
        context.dispatcher.run_async(
            update.message.reply_text,
            text=f'Теперь вы будете ежедневно оповещаться в {update.message.text}',
            reply_markup=settings_markup(
                get_user_notify_mode(update.message.from_user.id)),
            update=update
        )
        return cns.START_SETTINGS_HANDLER

    except KeyError:
        if update.message.text in cns.PLAIN_MENU_BUTTONS:
            fallback(update, context)
            return ConversationHandler.END
        update.message.delete()
        return cns.OFFSET_TIME_SETTINGS_HANDLER
    except Exception as e:
        logger.error(str(e), exc_info=True)


def db_set_specific_time_news_settings(engine: sqlalchemy.engine.Engine, time, user_id):
    with engine.begin() as conn:
        job_id = create_at_job(user_id, time, mode=cns.ENABLED_NEWS_NOTIFICATION)
        user_time = datetime.datetime.strptime(time, '%H:%M')
        group_names_query = conn.execute(sqlalchemy.text(
            "SELECT news_job_id "
            "FROM users.usergroup "
            "WHERE user_id=:uid "
            "AND news_job_id IS NOT NULL"),
            uid=user_id
        )
        if group_names_query.rowcount != 0:
            old_job_id = group_names_query.fetchone()['news_job_id']
            call(f'at -r {old_job_id}', shell=True)
        return conn.execute(sqlalchemy.text(
            "UPDATE users.usergroup "
            "SET (send_news_time, news_job_id, send_news_immediately)"
            " = (:snt,:jid, false) "
            "WHERE user_id=:uid"),
            uid=user_id,
            snt=user_time.time(),
            jid=int(job_id)
        )


def proceed_news_specific_time_settings(update: Update, context: CallbackContext):
    try:
        context.dispatcher.run_async(
            db_set_specific_time_news_settings,
            engine,
            update.message.text,
            update.message.from_user.id,
            update=update
        )
        global settings_state_dict
        settings_state_dict[update.message.from_user.id].news = cns.ENABLED_NEWS_NOTIFICATION
        context.dispatcher.run_async(
            update.message.reply_text,
            text=f'Теперь вы будете ежедневно оповещаться в {update.message.text}',
            reply_markup=settings_markup(
                get_user_notify_mode(update.message.from_user.id)),
            update=update
        )

        return cns.START_SETTINGS_HANDLER

    except ValueError:
        if update.message.text in cns.PLAIN_MENU_BUTTONS:
            fallback(update, context)
            return ConversationHandler.END
        update.message.delete()
        return cns.OFFSET_TIME_SETTINGS_HANDLER
    except Exception as e:
        logger.error(str(e), exc_info=True)


def get_offset_date(user_id: int, input_time) -> str:
    try:
        current_study_week = get_current_week()
        with engine.begin() as conn:
            date_string = ''
            week_of_year = datetime.date.today().isocalendar()[1]
            file = open('./misc/sql/select/next_first_pair.sql')
            next_first_pair_query = conn.execute(sqlalchemy.text(
                file.read()), uid=user_id, week_num=current_study_week)
            if next_first_pair_query.rowcount != 0:
                tmp = next_first_pair_query.fetchone()
                week_of_year = datetime.date.today().isocalendar()[1]
                week = week_of_year - current_study_week + tmp['week']
                first_pair_time = datetime.datetime.strptime(
                    tmp['starttime'], '%H:%M')
                notify_time = datetime.datetime.min + \
                    (first_pair_time - input_time)
                actual_date = datetime.datetime.fromisocalendar(datetime.date.today().isocalendar(
                )[0], week, tmp['day']).replace(hour=notify_time.hour, minute=notify_time.minute)
                date_string = actual_date.strftime('%H:%M %d.%m.%Y')
    except Exception as e:
        logger.error(str(e), exc_info=True)
        pass
    return date_string


def db_set_offset_time_settings(engine: sqlalchemy.engine.Engine, date_string: str, user_time: str, user_id: int):
    with engine.begin() as conn:
        job_id = create_at_job(user_id, date_string,
                               mode=cns.ENABLED_SCHEDULE_NOTIFICATION)
        old_job_id_query = conn.execute(sqlalchemy.text(
            "SELECT job_id "
            "FROM users.usergroup "
            "WHERE user_id=:uid AND job_id IS NOT NULL"),
            uid=user_id
        )
        if old_job_id_query.rowcount != 0:
            old_job_id = old_job_id_query.fetchone()['job_id']
            call(f'at -r {old_job_id}', shell=True)
        return conn.execute(
            sqlalchemy.text(
                "UPDATE users.usergroup "
                "SET (send_msg_time, job_id, offset_time) = "
                "(NULL,:jid,:offset_time) "
                "WHERE user_id=:uid"
            ),
            uid=user_id,
            offset_time=user_time,
            jid=int(job_id)
        )


def proceed_offset_time_settings(update: Update, context: CallbackContext):
    try:
        user_time = datetime.datetime.strptime(update.message.text, '%H:%M')
        date_string = get_offset_date(
            user_id=update.message.from_user.id,
            input_time=user_time
        )
        context.dispatcher.run_async(
            db_set_offset_time_settings,
            engine,
            date_string,
            user_time,
            update.message.from_user.id,
            update=update
        )
        
        global settings_state_dict
        settings_state_dict[
            update.message.from_user.id
        ].schedule = cns.ENABLED_SCHEDULE_NOTIFICATION

        context.dispatcher.run_async(
            update.message.reply_text,
            text=f'Теперь вы будете ежедневно оповещаться за {update.message.text} до пары',
            reply_markup=settings_markup(
                get_user_notify_mode(update.message.from_user.id)),
            update=update
        )
        return cns.START_SETTINGS_HANDLER
    except ValueError:
        if update.message.text in cns.PLAIN_MENU_BUTTONS:
            fallback(update, context)
            return ConversationHandler.END
        update.message.delete()
        return cns.OFFSET_TIME_SETTINGS_HANDLER
    except Exception as e:
        logger.error('proceed_offset!!!! ' + str(e), exc_info=True)


def switch_files(mode: str) -> str:
    return 'send_schedule_daily.py' if mode == cns.ENABLED_SCHEDULE_NOTIFICATION else 'send_news_daily.py'


def create_at_job(user_id: int, time: str, mode=cns.ENABLED_SCHEDULE_NOTIFICATION) -> str:
    job_id = None
    tmp = tempfile.NamedTemporaryFile(mode='r+t')
    cmd = f'echo \"python3 {os.getcwd()}/{switch_files(mode)} {user_id}\" | at -m {time}'
    call(cmd, shell=True, stderr=tmp)
    tmp.seek(0)
    for line in tmp:
        if 'job' in line:
            job_id = line.split()[1]
    tmp.close()
    return job_id


def db_cancel_schedule_notifications(engine: sqlalchemy.engine.Engine, user_id: int):
    with engine.begin() as conn:
        job_id_query = conn.execute(sqlalchemy.text(
            "SELECT job_id FROM users.usergroup WHERE user_id=:uid AND job_id IS NOT NULL"), uid=user_id)
        if job_id_query.rowcount != 0:
            old_job_id = job_id_query.fetchone()['job_id']
            call(f'at -r {old_job_id}', shell=True)
        conn.execute(sqlalchemy.text(
            "UPDATE users.usergroup "
            "SET (send_msg_time, job_id, offset_time)"
            " = (NULL,NULL,NULL) "
            "WHERE user_id=:uid"),
            uid=user_id
        )


def cancel_schedule_notifications(query: CallbackQuery, context: CallbackContext):
    try:
        context.dispatcher.run_async(
            db_cancel_schedule_notifications,
            engine,
            query.from_user.id,
            update=query
        )
        global settings_state_dict
        settings_state_dict[query.from_user.id].schedule = cns.DISABLED_SCHEDULE_NOTIFICATION
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': 'Больше не присылаю уведомлений о расписании'},
            {'reply_markup': settings_markup(
                get_user_notify_mode(query.from_user.id))},
            update=query
        )
    except Exception as e:
        logger.error(str(e), exc_info=True)

    return cns.START_SETTINGS_HANDLER


def db_cancel_news_notifications(engine: sqlalchemy.engine.Engine, user_id: int):
    with engine.begin() as conn:
        job_id_query = conn.execute(sqlalchemy.text(
            "SELECT news_job_id FROM users.usergroup WHERE user_id=:uid AND news_job_id IS NOT NULL"), uid=user_id)
        if job_id_query.rowcount != 0:
            old_job_id = job_id_query.fetchone()['news_job_id']
            call(f'at -r {old_job_id}', shell=True)
        return conn.execute(sqlalchemy.text(
            "UPDATE users.usergroup "
            "SET (send_news_time, news_job_id, send_news_immediately)"
            " = (NULL,NULL, false) "
            "WHERE user_id=:uid"),
            uid=user_id
        )


def cancel_news_notifications(query: CallbackQuery, context: CallbackContext):
    try:
        context.dispatcher.run_async(
            db_cancel_news_notifications,
            engine,
            query.from_user.id,
            update=query
        )
        global settings_state_dict
        settings_state_dict[query.from_user.id].news = cns.DISABLED_NEWS_NOTIFICATION

        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': 'Больше не присылаю уведомлений о новостях'},
            {'reply_markup': settings_markup(
                get_user_notify_mode(query.from_user.id))},
            update=query
        )
    except Exception as e:
        logger.error(str(e), exc_info=True)

    return cns.START_SETTINGS_HANDLER


def button(update: Update, context: CallbackContext):
    query = update.callback_query
    chosen_time = query.data
    current_week = get_current_week()
    query.answer()

    if chosen_time == cns.WEEK_SCHEDULE:
        current_user_timetable = get_user_week_timetable(
            query.from_user.id, current_week, is_rest_week=True)
        if not current_user_timetable:
            msg_to_user = 'Сейчас ' + \
                str(current_week) + \
                ' неделя.\nЗанятий на этой неделе больше не будет\n\n'
            next_week_with_classes = current_week
            while not current_user_timetable and next_week_with_classes <= 18:
                next_week_with_classes += 1
                current_user_timetable = get_user_week_timetable(
                    query.from_user.id, next_week_with_classes, is_rest_week=False)
            if next_week_with_classes <= 18:
                msg_to_user += 'Занятия на ' + \
                    str(next_week_with_classes) + ' неделю:\n'
                for msg in current_user_timetable:
                    msg_to_user += msg + "\n\n"

            context.dispatcher.run_async(
                edit_message_text_and_markup_async,
                query,
                {'text': msg_to_user},
                {'reply_markup': timetable_markup(chosen_time)},
                update=update
            )
        else:
            msg_to_user = 'Сейчас ' + \
                (cns.CREDIT_WEEK if current_week ==
                 18 else f'{current_week} неделя\n\n')
            for msg in current_user_timetable:
                msg_to_user += msg + "\n\n"

            context.dispatcher.run_async(
                edit_message_text_and_markup_async,
                query,
                {'text': msg_to_user},
                {'reply_markup': timetable_markup(chosen_time)},
                update=update
            )
    elif chosen_time == cns.DAY_SCHEDULE:
        current_user_timetable = get_user_day_timetable(query.from_user.id)
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': f"Сейчас {current_week} неделя\n\n" +
                current_user_timetable if current_user_timetable is not None else cns.USER_FREE_DAY},
            {'reply_markup': timetable_markup(chosen_time)},
            update=update
        )
    elif chosen_time == cns.SPECIFIC_WEEK_SCHEDULE:
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': f"Сейчас {current_week} неделя\n\nВыберите номер недели:"},
            {'reply_markup': weeks_num_markup()},
            update=query
        )
        return cns.SPECIFIC_WEEK_SCHEDULE_HANDLER
    return cns.SCHEDULE_MENU_HANDLER


def settings_controller(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    if query.data == cns.DISABLED_SCHEDULE_NOTIFICATION:     # SEND AT SPECIFIC TIME
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': "Ну выбери ты уже пункт меню\n"},
            {'reply_markup': schedule_notification_settings_markup()},
            update=update
        )
        return cns.SETTINGS_CONTROLLER_HANDLER
    elif query.data == cns.ENABLED_SCHEDULE_NOTIFICATION:    # DON'T SEND NOTIFICATIONS TO USER
        return cancel_schedule_notifications(query, context)
    elif query.data == cns.DISABLED_NEWS_NOTIFICATION:
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': "Ну выбери ты уже пункт меню\n"},
            {'reply_markup': news_notification_settings_markup()},
            update=update
        )
        return cns.SETTINGS_CONTROLLER_HANDLER
        # return send_user_request_of_specific_time(query, context)
    elif query.data == cns.ENABLED_NEWS_NOTIFICATION:
        return cancel_news_notifications(query, context)


def subscribe_user_to_immediate_news(query: CallbackQuery, context: CallbackContext) -> str:
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "UPDATE users.usergroup "
            "SET (send_news_time, news_job_id, send_news_immediately)"
            " = (NULL, NULL, true) "
            "WHERE user_id=:uid"
            ),
            uid=query.from_user.id
        )
    global settings_state_dict
    settings_state_dict[query.from_user.id].news = cns.ENABLED_NEWS_NOTIFICATION
    context.dispatcher.run_async(
        edit_message_text_and_markup_async,
        query,
        {'text': 'Вы будете получать уведомления о новостях'},
        {'reply_markup': settings_markup(
            get_user_notify_mode(query.from_user.id))},
        update=query
    )
    return cns.START_SETTINGS_HANDLER


def norification_settings_controller(update: Update, context: CallbackContext) -> str:
    query = update.callback_query
    query.answer()
    if query.data == cns.SPECIFY_SEND_MSG_TIME or query.data == cns.SPECIFY_SEND_NEWS_TIME:
        return send_user_request_of_specific_time(query, context)
    elif query.data == cns.SPECIFY_SEND_MSG_TIME_OFFSET:
        return send_user_request_of_offset_time(query, context)
    elif query.data == cns.SEND_NEWS_IMMEDIATELY:
        return subscribe_user_to_immediate_news(query, context)
    elif query.data == cns.BACK_TO_SETTINGS:
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': "Настройки подписок"},
            {'reply_markup': settings_markup(
                get_user_notify_mode(query.from_user.id))},
            update=update
        )
        return cns.START_SETTINGS_HANDLER
    elif query.data == cns.BACK_TO_SCHEDULE_SETTINGS:
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': "Ну выбери ты уже пункт меню\n"},
            {'reply_markup': schedule_notification_settings_markup()},
            update=update
        )
        return cns.SETTINGS_CONTROLLER_HANDLER
    elif query.data == cns.BACK_TO_NEWS_SETTINGS:
        context.dispatcher.run_async(
            edit_message_text_and_markup_async,
            query,
            {'text': "Ну выбери ты уже пункт меню\n"},
            {'reply_markup': news_notification_settings_markup()},
            update=update
        )
        return cns.SETTINGS_CONTROLLER_HANDLER


def send_user_request_of_specific_time(query: CallbackQuery, context: CallbackContext) -> str:
    context.dispatcher.run_async(
        edit_message_text_and_markup_async,
        query,
        {'text': 'Введите желаемое время\nФормат: hh:mm'},
        {
            'reply_markup': InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        "Назад",
                        callback_data=cns.BACK_TO_SCHEDULE_SETTINGS
                        if query.data == cns.SPECIFY_SEND_MSG_TIME
                        else cns.BACK_TO_NEWS_SETTINGS
                        if query.data == cns.SPECIFY_SEND_NEWS_TIME
                        else cns.BACK_TO_SETTINGS
                    )]
                ]
            )
        },
        update=query
    )

    return cns.SCHEDULE_SPECIFIC_TIME_SETTINGS_HANDLER \
        if query.data == cns.SPECIFY_SEND_MSG_TIME \
        else cns.NEWS_SPECIFIC_TIME_SETTINGS_HANDLER


def send_user_request_of_offset_time(query: CallbackQuery, context: CallbackContext) -> str:
    context.dispatcher.run_async(
        edit_message_text_and_markup_async,
        query,
        {'text': 'Введите offset времени перед парами\nФормат: hh:mm'},
        {
            'reply_markup': InlineKeyboardMarkup(
                [[InlineKeyboardButton(
                    "Назад", callback_data=cns.BACK_TO_SCHEDULE_SETTINGS)]]
            )
        },
        update=query
    )

    return cns.OFFSET_TIME_SETTINGS_HANDLER


def fallback(update: Update, context: CallbackContext):
    context.update_queue.put(update)
    return ConversationHandler.END


def proceed_specific_week_schedule(update: Update, context: CallbackContext) -> str:
    try:
        query = update.callback_query
        # I think this solution will be faster that subscribing cns.SPECIFIC_WEEK_SCHEDULE_HANDLER to button
        if query.data == cns.DAY_SCHEDULE:
            button(update, context)
        else:
            query.answer(text='👌🏿')
            # delete substr WEEK
            chosen_week = int(query.data[len('WEEK'):])
            current_user_timetable = get_user_week_timetable(
                query.from_user.id, chosen_week, is_rest_week=False)
            if not current_user_timetable:
                context.dispatcher.run_async(
                    edit_message_text_and_markup_async,
                    query,
                    {'text': f'Занятий на {chosen_week} неделе не будет'},
                    {'reply_markup': InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Назад", callback_data=cns.DAY_SCHEDULE)]])},
                    update=update
                )
            else:
                msg_to_user = (
                    cns.CREDIT_WEEK + '\n\n') if chosen_week == 18 else f'{chosen_week} неделя\n\n'
                for msg in current_user_timetable:
                    msg_to_user += msg + "\n\n"
                context.dispatcher.run_async(
                    edit_message_text_and_markup_async,
                    query,
                    {'text': msg_to_user},
                    {'reply_markup': InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Назад", callback_data=cns.DAY_SCHEDULE)]])},
                    update=update
                )
        return cns.SCHEDULE_MENU_HANDLER
    except Exception as e:
        logger.error(str(e), exc_info=True)


def proceed_map(update: Update, context: CallbackContext):
    with open('misc/img/nstu_map.jpg', 'rb') as f:
        update.message.reply_photo(photo=f, parse_mode='HTML')


def remove_html_tags(data: str) -> str:
    p = re.compile(r'<img.*?/>|<br />')
    return p.sub('', data)


def my_error_handler(update: Update, context: CallbackContext):
    """Log Errors caused by Updates."""
    try:
        raise context.error
    except (ValueError, KeyError):
        if update.message.text in cns.PLAIN_MENU_BUTTONS:
            fallback(update, context)
            return ConversationHandler.END
        update.message.delete()
        return cns.OFFSET_TIME_SETTINGS_HANDLER
    except Exception as e:
        logger.warning('Update "%s" caused error "%s"', update, e, exc_info=True)


def main():

    my_persistence = PicklePersistence(filename='persist.backup')
    updater = Updater(config.bot_token,
                      persistence=my_persistence, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # we'll put handlers to the misc folder in the future.
    schedule_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(Filters.text(cns.SCHEDULE_BUTTON_TEXT) & (
            ~Filters.command), proceed_timetable)],
        states={
            cns.SCHEDULE_MENU_HANDLER: [CallbackQueryHandler(button, pattern=r"\w*SCHEDULE$")],
            cns.SPECIFIC_WEEK_SCHEDULE_HANDLER: [CallbackQueryHandler(
                proceed_specific_week_schedule, pattern=fr'^(WEEK([1-9]|1[0-8])|{cns.DAY_SCHEDULE})$')]
        },
        allow_reentry=False,
        name='SCHEDULE_CONVERSATION_HANDLER',
        persistent=True,
        fallbacks=[MessageHandler(Filters.text(cns.PLAIN_MENU_BUTTONS), fallback)]
    )

    change_group_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler(['start', 'restart'], start),
            MessageHandler(
                Filters.text([cns.CHANGE_GROUP_BUTTON_TEXT]) & (~Filters.command),
                change_user_group)],

        states={
            cns.CLAIM_USER_GROUP_HANDLER: [MessageHandler(Filters.text & (~Filters.command), init_user)],
            cns.SET_USER_GROUP_HANDLER: [CallbackQueryHandler(
                select_group, pattern=r'^.*(-(\d*)|-.*(\d[а-яА-Я])|(ИДО)|(Аспиранты)|(ФДО)|(ЦМО)|(ИСР)|(группа))$')]
        },

        allow_reentry=False,
        name='CHANGE_GROUP_CONVERSATION_HANDLER',
        persistent=True,
        fallbacks=[CommandHandler('cancel', cancel), MessageHandler(
            Filters.text(cns.PLAIN_MENU_BUTTONS), cancel)]
    )

    settings_conversation_handler = ConversationHandler(
        entry_points=[
            MessageHandler(
                Filters.text(cns.NOTIFICATIONS_SETTINGS_BUTTON_TEXT) 
                & (~Filters.command),
                proceed_settings_start
            )
        ],
        states={
            cns.START_SETTINGS_HANDLER: [
                CallbackQueryHandler(
                    settings_controller,
                    pattern=r'^.*NOTIFICATION$'
                )
            ],
            cns.SETTINGS_CONTROLLER_HANDLER: [
                CallbackQueryHandler(
                    norification_settings_controller
                )
            ],
            cns.SCHEDULE_SPECIFIC_TIME_SETTINGS_HANDLER: [
                MessageHandler(
                    Filters.text & (~Filters.command),
                    proceed_schedule_specific_time_settings
                ),
                CallbackQueryHandler(
                    norification_settings_controller
                )
            ],
            cns.OFFSET_TIME_SETTINGS_HANDLER: [
                MessageHandler(
                    Filters.text & (~Filters.command),
                    proceed_offset_time_settings
                ),
                CallbackQueryHandler(
                    norification_settings_controller
                )
            ],
            cns.NEWS_SPECIFIC_TIME_SETTINGS_HANDLER: [
                MessageHandler(
                    Filters.text & (~Filters.command),
                    proceed_news_specific_time_settings
                ),
                CallbackQueryHandler(norification_settings_controller)
            ]
        },
        allow_reentry=False,
        name='SETTINGS_CONVERSATION_HANDLER',
        persistent=True,
        fallbacks=[MessageHandler(Filters.text(cns.PLAIN_MENU_BUTTONS), fallback)]
    )

    news_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(
            Filters.text(cns.NEWS_BUTTON_TEXT) & (~Filters.command),
            proceed_news
        )],
        states={
            cns.NEWS_MENU_HANDLER: [
                CallbackQueryHandler(
                    news_button_switch,
                    pattern=r"\w*NEWS$"
                )
            ],
            cns.SPECIFIC_DATE_NEWS_HANDLER: [
                MessageHandler(
                    Filters.text & (~Filters.command),
                    proceed_date_news
                ),
                CallbackQueryHandler(
                    news_button_switch,
                    pattern=r"\w*NEWS$"
                )
            ]
        },
        allow_reentry=False,
        name='NEWS_CONVERSATION_HANDLER',
        persistent=True,
        fallbacks=[MessageHandler(Filters.text(cns.PLAIN_MENU_BUTTONS), fallback)]
    )

    map_handler = MessageHandler(Filters.text(
        cns.MAP_BUTTON_TEXT) & (~Filters.command), proceed_map)

    dp.add_handler(schedule_conv_handler)
    dp.add_handler(settings_conversation_handler)
    dp.add_handler(news_conv_handler)
    dp.add_handler(change_group_conv_handler)
    dp.add_handler(map_handler)
    dp.add_error_handler(my_error_handler)
    # Start the Bot
    # updater.start_polling()
    # updater.start_webhook(
    #   listen='127.0.0.1',
    #   port=8443,
    #   url_path=config.bot_token
    # )
    # updater.bot.set_webhook(
    #     webhook_url='https://tg.btrd.tk/' + config.bot_token,
    #     certificate=open('cert.pem', 'rb')
    # )

    updater.start_webhook(
        listen='0.0.0.0',
        port=config.WEBHOOK_PORT,
        url_path=config.bot_token,
        key='private.key',
        cert='cert.pem',
        webhook_url=f'https://{config.SERVER_IP_ADDRESS}:'
                    f'{config.WEBHOOK_PORT}/{config.bot_token}'
    )

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()

# TODO: use logging besides print()
