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
\echo 'LOADING database'


-- Creating Data Tables
CREATE TABLE dashboard_usage (
    view_args_id      VARCHAR(100)      NOT NULL,
    log_date          DATE              NOT NULL,
    territory         VARCHAR(20)       NOT NULL,
    team              VARCHAR(100)      NOT NULL,
    activity_count    INTEGER           NOT NULL,
    active_users      INTEGER           NOT NULL
--    PRIMARY KEY (event_key)
) PARTITION BY RANGE (log_date);
--
---- Sample Data
--
--COPY public.t_request (method, path, endpoint, status,log_user,log_ts,browser,browser_platform,browser_version,log_vargs_id,event_key) FROM stdin;
--GET /dashboards/marketing-crm-scorecard dashboards.view_dashboard 200 giorgos.athineou@project-a.com 2019-08-22T08:50:22.526701 chrome macos 76.0.3809.100 marketing-crm-scorecard 1
--POST /dashboards/marketing-crm-scorecard dashboards.view_dashboard 200 giorgos.athineou@project-a.com 2019-08-22T08:50:22.526701 chrome macos 76.0.3809.100 marketing-crm-scorecard 2
--
