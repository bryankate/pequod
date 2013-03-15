
CREATE TABLE articles (
    aid integer NOT NULL,
    author integer,
    link varchar2(1024)
);

CREATE TABLE comments (
    cid integer NOT NULL,
    aid integer,
    commenter integer,
    "COMMENT" varchar2(1024)
);

CREATE TABLE votes (
    aid integer,
    voter integer
);
