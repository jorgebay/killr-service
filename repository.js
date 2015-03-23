"use strict";
var cassandra = require('cassandra-driver');

var cql = {
  selectCommentsByVideo: "SELECT videoid, commentid, userid, comment FROM comments_by_video WHERE videoid = ?",
  insertCommentByVideo: 'INSERT INTO comments_by_video (videoid, commentid, userid, comment) VALUES (?, ?, ?, ?)',
  insertCommentByUser: 'INSERT INTO comments_by_user (userid, commentid, videoid, comment) VALUES (?, ?, ?, ?)'
};

/**
 * Groups the logic to
 * @param {Client} client Cassandra client instance.
 * @param {Bus} bus Message broker instance.
 * @constructor
 */
function Repository(client, bus) {
  this.client = client;
  this.bus = bus;
}

/**
 * Gets an array of comments
 * @param {Uuid|String} id
 * @param {Function} callback
 */
Repository.prototype.getCommentsByVideo = function (id, callback) {
  var query = cql.selectCommentsByVideo;
  this.client.execute(query, [id], { prepare: true }, function (err, result) {
    if (err) return callback(err);
    callback(null, result.rows);
  });
};

/**
 * Inserts the a new comment (partitioned by video and user)
 */
Repository.prototype.insertComment = function (videoId, userId, comment, callback) {
  //TODO: validate parameters
  var commentId = cassandra.types.TimeUuid.now();
  var queries = [
    { query: cql.insertCommentByVideo, params: [videoId, commentId, userId, comment]},
    { query: cql.insertCommentByUser, params: [userId, commentId, videoId, comment]}
  ];
  var self = this;
  this.client.batch(queries, { prepare: true }, function (err) {
    if (!err) {
      //Notify using the message broker
      self.bus.publishNewComment(videoId, commentId, userId, comment);
    }
    callback(err, commentId);
  });
};

module.exports = Repository;