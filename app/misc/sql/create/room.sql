CREATE TABLE test.room
(
    pk integer NOT NULL,
    is_delete boolean,
    remark character varying COLLATE pg_catalog."default",
    name character varying COLLATE pg_catalog."default",
    not_study boolean,
    fk_building integer,
    is_nstu boolean,
    num character varying COLLATE pg_catalog."default",
    fk_type integer,
    date_apply timestamp without time zone,
    date_add timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT room_pkey PRIMARY KEY (pk)
)