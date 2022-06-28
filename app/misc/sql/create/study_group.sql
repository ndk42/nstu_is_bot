CREATE TABLE test.study_group
(
    pk integer NOT NULL,
    name character varying COLLATE pg_catalog."default",
    fk_training_forms integer,
    fk_curriculum integer,
    fsk_facultet integer,
    fk_facultet_filials integer,
    forming_date timestamp without time zone,
    deforming_date timestamp without time zone,
    last_modified timestamp without time zone,
    date_add timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT study_group_pkey PRIMARY KEY (pk)
)