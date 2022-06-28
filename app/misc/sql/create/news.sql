create table test.news
(
    id        integer                                not null
        constraint news_pkey
            primary key,
    url       varchar,
    title     varchar,
    shorttext varchar,
    news_date timestamp with time zone,
    date_add  timestamp with time zone default now() not null
);