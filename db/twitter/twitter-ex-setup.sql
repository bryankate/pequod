-- This file is meant to be run manually; it contains the set up for twitter on
-- both PostgrSQL and Oracle


-- PostgeSQL: Generate tables and triggers
-- Before running this sql, set Postgres with:
--mkdir /mnt/tmp
--mount -t tmpfs -o size=1024m tmpfs /mnt/tmp
--chown -R postgres:postgres /mnt/tmp
--su - postgres -c "/usr/lib/postgresql/9.1/bin/initdb -D /mnt/tmp"
-- in file '/mnt/tmp/postgresql.conf' set:
  -- fsync off
  -- synchronous commit off
  -- full_pages_writes off
  -- port 5477 (so as not to conflict with other postgres if you have one running)
  -- bgwriter_lru_maxpages=0 (???)
--su - postgres -c "/usr/lib/postgresql/9.1/bin/pg_ctl -D /mnt/tmp -l /mnt/tmp/postgres.log start"
--OR
--su - postgres -c "numactl -C 5 /usr/lib/postgresql/9.1/bin/pg_ctl -D /mnt/tmp -l /mnt/tmp/postgres.log start"

DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS p;
DROP TABLE IF EXISTS t;
DROP TRIGGER IF EXISTS update_timeline;

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
	time TIMESTAMP,
	CONSTRAINT primary_p PRIMARY KEY (poster,time)
);
CREATE INDEX p_index_time ON p (time);
CREATE INDEX p_index_poster ON p (poster);

CREATE TABLE t (
	usr int,
	poster int,
	value varchar(140),
	time timestamp
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

CREATE TRIGGER update_timeline
AFTER INSERT ON p
FOR EACH ROW
EXECUTE PROCEDURE push_posts();

-- Oracle: Generate tables and materialized view

-- turn on locking, needed to drop and build the tables
ALTER system SET dml_locks = 1100 SCOPE=SPFile;
SHUTDOWN ABORT;
STARTUP;

-- roll your own 'IF EXISTS'
CREATE OR REPLACE PROCEDURE drop_if_exists(tbl IN VARCHAR2) IS 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE ' || tbl || ' CASCADE CONSTRAINTS';
   commit;
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- roll your own 'IF EXISTS'
CREATE OR REPLACE PROCEDURE drop_mv_if_exists(tbl IN VARCHAR2) IS 
BEGIN
   EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW ' || tbl;
   commit;
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -12003 THEN
         RAISE;
      END IF;
END;
/

execute drop_if_exists('s');
execute drop_if_exists('p');
execute drop_mv_if_exists('t');

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
	time TIMESTAMP,
	CONSTRAINT primary_p PRIMARY KEY (poster,time)

);
CREATE INDEX p_index_time ON p (time);
CREATE INDEX p_index_poster ON p (poster);

CREATE MATERIALIZED VIEW LOG ON s WITH ROWID;
CREATE MATERIALIZED VIEW LOG ON p WITH ROWID;

CREATE MATERIALIZED VIEW t
NOLOGGING
CACHE
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
AS
SELECT 
	s.ROWID as s_rowid, p.ROWID as p_rowid, s.usr AS usr, p.poster AS poster, p.value AS value, p.time AS time
FROM 
	s, p
WHERE
	s.poster = p.poster;

CREATE INDEX t_index_usr ON t (usr);
CREATE INDEX t_index_time ON t (time);

-- 'read committed' is the default isolation level in Oracle. It is also allegedly nonblocking so we don't need to make a change here (we would go for 'read uncommitted' on other systems)
-- turn off/down locking. This will break a number of things: CREATE INDEX, DROP TABLE, and LOCK TABLE
ALTER system SET dml_locks = 0 SCOPE=SPFile;
-- Loosen durability. This will batch multiple transactions and return immediately after the commit is issued
ALTER system SET commit_write = 'BATCH,NOWAIT' SCOPE=BOTH;
SHUTDOWN IMMEDIATE;
STARTUP;


