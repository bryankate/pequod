--
-- Trigger functions for karma
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

DROP TABLE IF EXISTS matviews CASCADE;
DROP INDEX IF EXISTS author_idx;

CREATE TABLE matviews (
  mv_name NAME NOT NULL PRIMARY KEY
  , v_name NAME NOT NULL
  , last_refresh TIMESTAMP WITH TIME ZONE
);

CREATE OR REPLACE FUNCTION create_matview(NAME, NAME)
 RETURNS VOID
 SECURITY DEFINER
 LANGUAGE plpgsql AS '
 DECLARE
     matview ALIAS FOR $1;
     view_name ALIAS FOR $2;
     entry matviews%ROWTYPE;
 BEGIN
     SELECT * INTO entry FROM matviews WHERE mv_name = matview;
 
     IF FOUND THEN
         RAISE EXCEPTION ''Materialized view ''''%'''' already exists.'',
           matview;
     END IF;
 
     EXECUTE ''REVOKE ALL ON '' || view_name || '' FROM PUBLIC''; 
 
     EXECUTE ''GRANT SELECT ON '' || view_name || '' TO PUBLIC'';
 
     EXECUTE ''CREATE TABLE '' || matview || '' AS SELECT * FROM '' || view_name;
 
     EXECUTE ''REVOKE ALL ON '' || matview || '' FROM PUBLIC'';
 
     EXECUTE ''GRANT SELECT ON '' || matview || '' TO PUBLIC'';
 
     INSERT INTO matviews (mv_name, v_name, last_refresh)
       VALUES (matview, view_name, CURRENT_TIMESTAMP); 
     
     RETURN;
 END
 ';

DROP TABLE IF EXISTS karma_mv CASCADE;

SELECT create_matview('karma_mv', 'karma_v');

CREATE INDEX author_idx ON karma_mv(author);

CREATE OR REPLACE FUNCTION karma_mv_refresh_row(karma_mv.author%TYPE) RETURNS VOID
SECURITY DEFINER
LANGUAGE 'plpgsql' AS '
BEGIN
  DELETE FROM karma_mv WHERE author = $1;
  INSERT INTO karma_mv SELECT * FROM karma_v WHERE author = $1;
  RETURN;
END
';


CREATE OR REPLACE FUNCTION karma_mv_article_it() RETURNS TRIGGER
SECURITY DEFINER LANGUAGE 'plpgsql' AS '
BEGIN
  PERFORM karma_mv_refresh_row(articles.author) FROM articles WHERE articles.author = NEW.author;
  RETURN NULL;
END
';

DROP TRIGGER IF EXISTS karma_mv_articles_trigger ON articles;

CREATE TRIGGER karma_mv_articles_trigger AFTER INSERT ON articles
  FOR EACH ROW EXECUTE PROCEDURE karma_mv_article_it(); 


CREATE OR REPLACE FUNCTION karma_mv_vote_it() RETURNS TRIGGER
SECURITY DEFINER LANGUAGE 'plpgsql' AS '
BEGIN
  PERFORM karma_mv_refresh_row(articles.author) FROM articles WHERE articles.aid = NEW.aid;
  RETURN NULL;
END
';

DROP TRIGGER IF EXISTS karma_mv_votes_trigger on votes;

CREATE TRIGGER karma_mv_votes_trigger AFTER INSERT ON votes
  FOR EACH ROW EXECUTE PROCEDURE karma_mv_vote_it();