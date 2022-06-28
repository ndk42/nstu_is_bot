CREATE TABLE test.type_study_work
(
    pk integer NOT NULL,
    name character varying COLLATE pg_catalog."default",
    short_name character varying COLLATE pg_catalog."default",
    is_session boolean,
    complexity integer,
    date_add timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT type_study_work_pkey PRIMARY KEY (pk)
)