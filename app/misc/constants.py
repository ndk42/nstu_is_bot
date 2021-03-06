USER_FREE_DAY = "Сегодня не учишься, угомонись"
EMPTY_NEWS = 'На этот день у нас нет новостей'
CREDIT_WEEK = '18 (зачетная) неделя\nУточняйте расписание у преподавателей и в личном кабинете студента НГТУ'
TIMETABLE_NAME = "test.tt_new"
NEWS_BUTTON_TEXT, NOTIFICATIONS_SETTINGS_BUTTON_TEXT, MAP_BUTTON_TEXT = 'Новости', 'Подписки', 'Карта НГТУ'
SCHEDULE_BUTTON_TEXT, CHANGE_GROUP_BUTTON_TEXT = 'Расписание', 'Сменить группу'
MENU_BUTTONS = [[SCHEDULE_BUTTON_TEXT, NEWS_BUTTON_TEXT], [MAP_BUTTON_TEXT, NOTIFICATIONS_SETTINGS_BUTTON_TEXT], [CHANGE_GROUP_BUTTON_TEXT]]
PLAIN_MENU_BUTTONS = [SCHEDULE_BUTTON_TEXT, NEWS_BUTTON_TEXT, MAP_BUTTON_TEXT, NOTIFICATIONS_SETTINGS_BUTTON_TEXT, CHANGE_GROUP_BUTTON_TEXT]
DISABLED_SCHEDULE_NOTIFICATION, ENABLED_SCHEDULE_NOTIFICATION = 'DISABLED_SCHEDULE_NOTIFICATION', 'ENABLED_SCHEDULE_NOTIFICATION'
DISABLED_NEWS_NOTIFICATION, ENABLED_NEWS_NOTIFICATION = 'DISABLED_NEWS_NOTIFICATION', 'ENABLED_NEWS_NOTIFICATION'
DAY_SCHEDULE, WEEK_SCHEDULE, SPECIFIC_WEEK_SCHEDULE = 'DAY_SCHEDULE', 'WEEK_SCHEDULE', 'SPECIFIC_WEEK_SCHEDULE'     # map(str, range(100,103))
DAY_NEWS, LAST_FIVE_NEWS, SPECIFIC_DATE_NEWS = 'DAY_NEWS', 'LAST_FIVE_NEWS', 'SPECIFIC_DATE_NEWS'
SPECIFY_SEND_MSG_TIME, SPECIFY_SEND_MSG_TIME_OFFSET = 'SPECIFY_SEND_MSG_TIME', 'SPECIFY_SEND_MSG_TIME_OFFSET'
SPECIFY_SEND_NEWS_TIME, SEND_NEWS_IMMEDIATELY = 'SPECIFY_SEND_NEWS_TIME', 'SEND_NEWS_IMMEDIATELY'
BACK_TO_SCHEDULE_SETTINGS = '-3'
BACK_TO_NEWS_SETTINGS = '-4'
BACK_TO_SETTINGS = '-5'
START_SETTINGS_HANDLER, SCHEDULE_SPECIFIC_TIME_SETTINGS_HANDLER, SETTINGS_CONTROLLER_HANDLER, \
OFFSET_TIME_SETTINGS_HANDLER, CLAIM_USER_GROUP_HANDLER, SET_USER_GROUP_HANDLER, \
SCHEDULE_MENU_HANDLER, SPECIFIC_WEEK_SCHEDULE_HANDLER, NEWS_MENU_HANDLER, \
SPECIFIC_DATE_NEWS_HANDLER, NEWS_SPECIFIC_TIME_SETTINGS_HANDLER = range(5000, 5011)

