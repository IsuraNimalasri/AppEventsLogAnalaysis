-----------------------------------------------------------------------------
-- Name : Isura Nimalasiri
-- Date : 14 Nov 2001
-- Description : DB script for creating table schema for request.zip data
--------------------------------------------------------------------------------

--- Setup DB Configurations

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--Setup database
DROP DATABASE IF EXISTS utracker;
CREATE DATABASE utracker;
\c utracker;

-- DIM TABLES

CREATE TABLE IF NOT EXISTS public.t_dim_user (
   "user_dimkey" varchar NOT NULL,
   "u_domain" varchar NOT NULL,
   "u_type" varchar  NOT NULL,
   "flag" varchar NOT NULL DEFAULT 'Y'
   );

CREATE TABLE IF NOT EXISTS public.t_dim_browser (
   "b_dimkey" varchar NOT NULL,
   "b_name" varchar NOT NULL,
   "b_os" varchar NOT NULL,
   "b_vrs" varchar  NOT NULL,
   "flag" varchar NOT NULL DEFAULT 'Y'
   );

 CREATE TABLE IF NOT EXISTS public.t_dim_dashboards (
   "db_dimkey" varchar NOT NULL,
   "db_name" varchar NOT NULL,
   "db_endpoint" varchar NOT NULL,
   "db_type" varchar  NOT NULL,
   "flag" varchar NOT NULL DEFAULT 'Y'
   );

 CREATE TABLE IF NOT EXISTS public.t_dim_time (
   "time_dimkey" bigint NOT NULL,
   "req_ts" varchar ,
   "req_dt" date NOT NULL,
   "req_y" int  NOT NULL,
   "req_m" int  NOT NULL,
   "req_d" int  NOT NULL,
   "req_h" int  NOT NULL,
   "req_dow" int  NOT NULL,
   "req_woy" int  NOT NULL,
   "flag" varchar NOT NULL DEFAULT 'Y'
   );

--FACT TABLES

CREATE TABLE IF NOT EXISTS public.t_fact_view_reqs (
   "vreq_factkey" varchar NOT NULL,
   "num_of_requests" int NOT NULL,
   "time_dimkey" bigint NOT NULL ,
   "user_dimkey" varchar NOT NULL ,
   "db_dimkey" varchar NOT NULL ,
   "b_dimkey" varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS public.t_fact_user_eng (
   "request_timestamp"  timestamp NOT NULL,
   "request_date" date NOT NULL,
   "user_event_value" int NOT NULL,
   "last_request_timestamp" timestamp NOT NULL,
   "last_request_date" date NOT NULL,
   "viewflow" int NOT NULL,
   "durationInSec" bigint,
   "time_dimkey" bigint NOT NULL ,
   "user_dimkey" varchar NOT NULL,
   "db_dimkey"  varchar NOT NULL,
   "b_dimkey"  varchar NOT NULL
);







