DROP KEYSPACE IF EXISTS killrvideo;
CREATE KEYSPACE killrvideo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use killrvideo;

// Counter table
CREATE TABLE video_rating (
   videoid uuid,
   rating_counter counter,
   rating_total counter,
   PRIMARY KEY (videoid)
);

// Video ratings by user (to try and mitigate voting multiple times)
CREATE TABLE video_ratings_by_user (
   videoid uuid,
   userid uuid,
   rating int,
   PRIMARY KEY (videoid, userid)
);

// Comments as a many-to-many
// Looking from the video side to many users
CREATE TABLE comments_by_video (
   videoid uuid,
   commentid timeuuid,
   userid uuid,
   comment text,
   PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

// looking from the user side to many videos
CREATE TABLE comments_by_user (
   userid uuid,
   commentid timeuuid,
   videoid uuid,
   comment text,
   PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);