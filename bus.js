"use strict";
var events = require('events');
var util = require('util');

/**
 * The client to the Message broker / middleware used to communicate with other services.
 * You can subscribe to events coming from the message broker used {@link https://nodejs.org/api/events.html#events_emitter_on_event_listener|EventEmitter `on` method}.
 * It can emit events via the method {@link Bus#publishNewComment}.
 * @constructor
 */
function Bus() {
  events.EventEmitter.call(this);
}

util.inherits(Bus, events.EventEmitter);

/**
 *
 */
Bus.prototype.publishNewComment = function () {
  //Not implemented
  //This method would publish() a new message in a AMQP server exchange
  //Other services can consume this type of messages to perform specific actions like:
  //  - Clean internal cache
  //  - Refresh internal state
  //  - Push a notification to another user
};

module.exports = Bus;