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

--ALTER TABLE public.articles OWNER TO neha;

--
-- Name: comments; Type: TABLE; Schema: public; Owner: neha; Tablespace: 
--

CREATE INDEX articles_author ON articles (author);
CREATE INDEX comments_aid ON comments (aid);
CREATE INDEX comments_commenter ON comments (commenter);
CREATE INDEX votes_aid ON votes (aid);

ALTER TABLE ONLY articles
    ADD CONSTRAINT articles_pkey PRIMARY KEY (aid);

ALTER TABLE ONLY comments
    ADD CONSTRAINT comments_pkey PRIMARY KEY (cid);

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--