"use strict";
var express = require('express');
var cassandra = require('cassandra-driver');
var bodyParser = require("body-parser");
var Repository = require('./repository');
var Bus = require('./bus');

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({
  contactPoints: [ contactPoint ],
  keyspace: 'killrvideo'
});
var bus = new Bus();
bus.on('video-deleted', function (videoId) {
  //A sample, we could use an "video deleted" event
  // to do something like clear an internal state
});
var repository = new Repository(client, bus);

var app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.get('/', function (req, res) {
  res.send('Service running');
});
app.get('/v1/comment/:videoId([a-f0-9\\-]{36})', function (req, res, next) {
  repository.getCommentsByVideo(req.params.videoId, function (err, comments) {
    if (err) return next(err);
    res.json(comments);
  });
});
app.post('/v1/comment/:videoId([a-f0-9\\-]{36})', function (req, res, next) {
  repository.insertComment(req.params.videoId, req.body.userId, req.body.comment, function (err, id) {
    if (err) return next(err);
    res.send(id.toString());
  });
});
app.get('/v1/rating/:videoId([a-f0-9\\-]{36})', function (req, res, next) {
  repository.getRating(req.params.videoId, function (err, rating) {
    if (err) return next(err);
    res.json(rating);
  });
});
app.post('/v1/rating/:videoId([a-f0-9\\-]{36})', function (req, res, next) {
  repository.setRating(req.params.videoId, req.body.userId, req.body.value, function (err) {
    if (err) return next(err);
    res.end();
  });
});
client.connect(function (err) {
  if (err) {
    console.error('Cassandra driver was not able to connect to %s: %s', contactPoint, err);
  }
  var server = app.listen(8080, function () {
    console.log('App listening at http://%s:%s', 'localhost', server.address().port);
  });
});