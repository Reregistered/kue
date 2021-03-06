 
/*!
 * kue - Worker
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
  , redis = require('../redis')
  , events = require('./events')
  , Job = require('./job');

/**
 * Expose `Worker`.
 */

module.exports = Worker;

/**
 * Redis connections used by `getJob()` when blocking.
 */

var clients = {};

console.log('KUE: Test init.');

/**
 * Initialize a new `Worker` with the given Queue
 * targetting jobs of `type`.
 *
 * @param {Queue} queue
 * @param {String} type
 * @api private
 */

var idx = 0;
function Worker(queue, type) {
  this.queue = queue;
  this.type = type;
  this.client = Worker.client || (Worker.client = redis.createClient());
  this.interval = 1000;
  this.idx = ++idx;
  this.log = function(msg) {
    console.log(msg + ' idx: ' + this.idx);
  };
  this.log('KUE: Worker');
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Worker.prototype.__proto__ = EventEmitter.prototype;

/**
 * Start processing jobs with the given `fn`,
 * checking for jobs every second (by default).
 *
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.start = function(fn){
  var self = this;
  this.log('KUE: Worker, before get job.');
  self.getJob(function(err, job){
    self.log('KUE: getJob err:', err);
    if (err) self.error(err, job);
    if (!job || err) return process.nextTick(function(){ self.start(fn); });
    self.process(job, fn);
  });
  return this;
};

/**
 * Error handler, currently does nothing.
 *
 * @param {Error} err
 * @param {Job} job
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.error = function(err, job){
  // TODO: emit non "error"
  this.emit('error', err);
  
  return this;
};

/**
 * Process a failed `job`. Set's the job's state
 * to "failed" unless more attempts remain, in which
 * case the job is marked as "inactive" and remains
 * in the queue.
 *
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.failed = function(job, err, fn){
  var self = this;
  events.emit(job.id, 'failed');
  job.failed().error(err);
  self.error(err, job);
  job.attempt(function(error, remaining, attempts, max){
    if (error) return self.error(error, job);
    remaining
      ? job.inactive()
      : job.failed();
    self.start(fn);
  });
};

/**
 * Process `job`, marking it as active,
 * invoking the given callback `fn(job)`,
 * if the job fails `Worker#failed()` is invoked,
 * otherwise the job is marked as "complete".
 *
 * @param {Job} job
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api public
 */

Worker.prototype.process = function(job, fn){
  var self = this
    , start = new Date;
  job.active();

  // add the job for event mapping
  events.add(job);

  fn(job, function(err, removed){
    if (err) return self.failed(job, err, fn);
    if(removed ) {
      self.emit('removed', job);
      job.remove();
    } else {
      job.complete();
      job.set('duration', job.duration = new Date - start);
      self.emit('job complete', job);
      events.emit(job.id, 'complete');
    }
    self.start(fn);
  });
  return this;
};

/**
 * Atomic ZPOP implementation.
 *
 * @param {String} key
 * @param {Function} fn
 * @api private
 */

Worker.prototype.zpop = function(key, fn){
  this.client
    .multi()
    .zrange(key, 0, 0)
    .zremrangebyrank(key, 0, 0)
    .exec(function(err, res){
      if (err) return fn(err);
      var id = res[0][0];
      fn(null, id);
    });
};

/**
 * Attempt to fetch the next job. 
 *
 * @param {Function} fn
 * @api private
 */

Worker.prototype.getJob = function(fn){
  var self = this;
  // workers cannot re-use the same blocking connection.
  // they each need their own.
  var clientKey = this.type + '_' + this.idx;

  // alloc a client for this job type
  var client = clients[clientKey]
    || (clients[clientKey] = redis.createClient());

  try{
    // BLPOP indicates we have a new inactive job to process
    client.blpop(['q:' + self.type + ':jobs', 0], function(err) {
      if (err) return fn(err);
      self.zpop('q:jobs:' + self.type + ':inactive', function(err, id){
        if (err) return fn(err);
        if (!id) return fn();
        Job.get(id, fn);
      });
    });

  }catch(err){
    return fn(err);
  }
};
