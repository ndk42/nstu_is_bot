CREATE TABLE test.pair
(
    pk integer NOT NULL,
	pair_number integer,
    begin_time character varying COLLATE pg_catalog."default",
	end_time character varying COLLATE pg_catalog."default",
	date_add timestamp without time zone NOT NULL DEFAULT now(),
	CONSTRAINT pair_pkey PRIMARY KEY (pk)
)