CREATE TABLE test.tt_cell_room
(
    pk integer NOT NULL,
    fk_room integer,
    lastmodified timestamp without time zone,
    fk_tt_cell integer,
    date_add timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT tt_cell_room_pkey PRIMARY KEY (pk)
)