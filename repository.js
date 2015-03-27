"use strict";
var cassandra = require('cassandra-driver');
var async = require('async');

var cql = {
  selectCommentsByVideo: 'SELECT videoid, commentid, userid, comment FROM comments_by_video WHERE videoid = ?',
  insertCommentByVideo: 'INSERT INTO comments_by_video (videoid, commentid, userid, comment) VALUES (?, ?, ?, ?)',
  insertCommentByUser: 'INSERT INTO comments_by_user (userid, commentid, videoid, comment) VALUES (?, ?, ?, ?)',
  selectRating: 'SELECT rating_counter, rating_total FROM video_rating WHERE videoid = ?',
  updateRating: 'UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + ? WHERE videoid = ?',
  insertRatingByUser: 'INSERT INTO video_ratings_by_user (videoid, userid, rating) VALUES (?, ?, ?) IF NOT EXISTS'
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

/**
 * Gets a rating for a given video
 * @param {Uuid|String} id Id of the video
 * @param {Function} callback
 */
Repository.prototype.getRating = function (id, callback) {
  this.client.execute(cql.selectRating, [id], { prepare: true }, function (err, result) {
    if (err) return callback(err);
    var rating = null;
    if (result.rows.length === 1) {
      var votes = result.rows[0]['rating_counter'];
      //counter values are retrieved as Long
      if (votes.greaterThan(0)) {
        rating = { avg: result.rows[0]['rating_total'].div(votes), votes: votes};
      }
    }
    callback(null, rating);
  });
};

/**
 * Sets a user rating for a given video
 * @param {Uuid|String} videoId Id of the video
 * @param {Uuid|String} userId Id of the user
 * @param {String} value Rating value
 * @param {Function} callback
 */
Repository.prototype.setRating = function (videoId, userId, value, callback) {
  var client = this.client;
  var applied;
  var ratingValue = cassandra.types.Long.fromString(value);
  async.series([
    function trackRating(next) {
      client.execute(cql.insertRatingByUser, [videoId, userId, ratingValue], { prepare: true}, function (err, result) {
        if (!err && result.rows.length === 1) {
          applied = result.rows[0]['[applied]'];
        }
        next(err);
      });
    },
    function updateRating(next) {
      if (!applied) {
        //The rating already exists for this user, nothing to do here
        return next();
      }
      client.execute(cql.updateRating, [ratingValue, videoId], { prepare: true}, next);
    }
  ], callback);
};

module.exports = Repository;