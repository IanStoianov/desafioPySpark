-- Table: public.breweries_raw

-- DROP TABLE IF EXISTS public.breweries_raw;

CREATE TABLE IF NOT EXISTS public.breweries_raw
(
    id text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default",
    brewery_type text COLLATE pg_catalog."default",
    address_1 text COLLATE pg_catalog."default",
    address_2 text COLLATE pg_catalog."default",
    address_3 text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    state_province text COLLATE pg_catalog."default",
    postal_code text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    longitude text COLLATE pg_catalog."default",
    latitude text COLLATE pg_catalog."default",
    phone text COLLATE pg_catalog."default",
    website_url text COLLATE pg_catalog."default",
    state text COLLATE pg_catalog."default",
    street text COLLATE pg_catalog."default"
);

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.breweries_raw
    OWNER to postgres;




