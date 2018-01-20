DROP TABLE IF EXISTS public.schema;
DROP TABLE IF EXISTS public.metric;
DROP TABLE IF EXISTS public.monitor;

CREATE TABLE public.schema
(
    id serial NOT NULL PRIMARY KEY,
    name character varying(32) NOT NULL,
    owner character varying(32) NOT NULL,
    environment character varying(64) NOT NULL,
    data json NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);

CREATE TABLE public.metric
(
    id serial NOT NULL PRIMARY KEY,
    name character varying(32) NOT NULL,
    owner character varying(32) NOT NULL,
    environment character varying(64) NOT NULL,
    data json NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);

CREATE TABLE public.monitor
(
    id serial NOT NULL PRIMARY KEY,
    name character varying(32) NOT NULL,
    owner character varying(32) NOT NULL,
    environment character varying(64) NOT NULL,
    data json NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);