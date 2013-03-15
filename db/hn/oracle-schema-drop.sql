--
-- Oracle HN Setup
--
CREATE OR REPLACE PROCEDURE drop_if_ex(tbl IN VARCHAR2) IS 
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

execute drop_if_ex('votes');
execute drop_if_ex('articles');
execute drop_if_ex('comments');


exit;
