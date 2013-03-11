--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

--COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: articles; Type: TABLE; Schema: public; Owner: neha; Tablespace: 
--

DROP TRIGGER IF EXISTS karma_mv_votes_trigger ON votes CASCADE;
DROP TABLE IF EXISTS matviews CASCADE;
DROP FUNCTION IF EXISTS update_karma(karma_mv.author%TYPE);
DROP VIEW IF EXISTS karma_v CASCADE;
DROP TABLE IF EXISTS articles CASCADE;
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS votes CASCADE;
DROP VIEW IF EXISTS karma_v CASCADE;

DROP TABLE IF EXISTS matviews CASCADE;
DROP INDEX IF EXISTS author_idx;
DROP FUNCTION IF EXISTS create_matview(NAME, NAME);
DROP FUNCTION IF EXISTS karma_mv_vote_it();
DROP FUNCTION IF EXISTS karma_mv_article_it();
DROP FUNCTION IF EXISTS karma_refresh_row();
DROP TABLE IF EXISTS karma_mv;
--
-- PostgreSQL database dump complete
--

