DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS p;
DROP TABLE IF EXISTS t;

CREATE TABLE s (
	usr INT,
	poster INT,
--	online BOOLEAN,
	CONSTRAINT primary_s PRIMARY KEY (usr,poster)
);
CREATE INDEX s_index_usr ON s (usr);
CREATE INDEX s_index_poster ON s (poster);
--CREATE INDEX s_index_online ON s (online);

CREATE TABLE p (
	poster INT,
	value VARCHAR(140),
	time INT,
	CONSTRAINT primary_p PRIMARY KEY (poster,time)
);
CREATE INDEX p_index_time ON p (time);
CREATE INDEX p_index_poster ON p (poster);

CREATE TABLE t (
	usr INT,
	poster INT,
	value varchar(140),
	time INT
);
CREATE INDEX t_index_usr ON t (usr);
CREATE INDEX t_index_time ON t (time);

-- Postgres doesn't support materialized views (until 9.3 which is in beta)
-- Instead we use a real table and update it using a trigger function 
CREATE OR REPLACE FUNCTION push_posts()
RETURNS TRIGGER AS $BODY$
BEGIN
    INSERT INTO t (usr, poster, value, time)
      SELECT s.usr, NEW.poster, NEW.value, NEW.time
      FROM s
      WHERE s.poster = NEW.poster
 --     AND s.online = TRUE
      ;
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

DROP TRIGGER IF EXISTS update_timeline ON p;
CREATE TRIGGER update_timeline
AFTER INSERT ON p
FOR EACH ROW
EXECUTE PROCEDURE push_posts();
