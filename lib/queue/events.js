/*!
 * kue - events
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var redis = require('../redis');

/**
 * Job map.
 */

exports.jobs = {};

/**
 * Pub/sub key.
 */

exports.key = 'q:events';

// pub client 
var pubClient;
var DEFAULT_EXPIRATION = 10;

/**
 * Add `job` to the jobs map, used
 * to grab the in-process object
 * so we can emit relative events.
 *
 * @param {Job} job
 * @api private
 */

exports.add = function(job, cb){
  if (job.id){
    if (job.id in exports.jobs){

      console.log('already registered');

    }else{
      exports.jobs[job.id] = job;
      exports.subscribe(job.id, cb);
    }
  }
};


var getNextSubKey = function(id){
  return exports.key + ':' + id + ':newsub';
};

var getSubsKey = function(id){
  return exports.key + ':' + id + ':subs';
};

var getClientSubKey = function(id, idx){
  return exports.key + ':' + id + ':sub:' + idx;
};

var getDefQueueKey = function(id){
  return exports.key + ':' + id + ':allevents';
};


/**
 * Subscribe to "q:events".
 *
 * @api private
 */

exports.subscribe = function(queueId, cb){

  cb = cb || function(){};

  var lClient = redis.createClient();
  if (!pubClient){
    pubClient = redis.createClient();
  }

  pubClient.incr(getNextSubKey(queueId), function(err,subval){

    // get the number of subs for this key
    var clientId = getClientSubKey(queueId,subval);

    var multi = pubClient.multi();
    multi.lrange([getDefQueueKey(queueId), 0, -1]);
    multi.sadd([getSubsKey(queueId),subval]);

    multi.exec(function(err, results){

      // add the results to our queue from behind
      for (var msgIdx = 0 ; msgIdx < results[0].length ; ++msgIdx) {
        pubClient.lpush(clientId, results[0][msgIdx]);
      }

      var handleMessage = function(channel, data){
        
        console.log('handle message called');

        // some attempt to free up this connection, not great practice
        // but blpop is blocking.
        var queueNext = true;
        if (data && data[1]){
          queueNext = ((JSON.parse(data[1]).event !== 'removed') &&
            (JSON.parse(data[1]).event !== 'complete'));
        }

        if (!queueNext){
          // extract my Id and unsubscribe me
          exports.unsubscribe(queueId,subval, function(){
            try{
              exports.onMessage(channel,data);
            }catch(err){
              // just continue
            }
          });

          lClient.end();
          delete lClient;
        } else {
          try{
            exports.onMessage(channel,data);
          }catch(err){
            // just continue
          }

          lClient.blpop([clientId,0],handleMessage);
        }
      };

      lClient.setex(['eventjs:blpop',1,100 ],function(){});
      lClient.blpop([clientId,0],handleMessage);

      cb(err,subval);

    });
  });
};


exports.unsubscribe = function(queueId, subval, cb){

  if (!pubClient){
    pubClient = redis.createClient();
  }

  pubClient.srem([getSubsKey(queueId),subval], function(err,dontcare){

    // we're removed from the list, clear up our queue
    // cheap hack - we know we don't reuse queue ID's so allow it to expire
    // after X seconds to catch any lingering events
    pubClient.expire([getClientSubKey(queueId,subval),DEFAULT_EXPIRATION], function(err,dontcare){

      // we're all cleaned up, return
      cb(err,subval);

      // custom code for our case - after the last user unsubscribes, get rid
      // of the counter - not really safe, but we shouldn't be in a situation
      // where new users are subscribing when old ones are dropping off.
      pubClient.scard([getSubsKey(queueId)], function(err,size){
        if (size == 0){
          var multi = pubClient.multi();
          multi.expire([getDefQueueKey(queueId),DEFAULT_EXPIRATION]);
          multi.expire([getNextSubKey(queueId),DEFAULT_EXPIRATION]);
          multi.exec(function(){});
        }
      });
    });
  });
};

/**
 * Message handler.
 *
 * @api private
 */

exports.onMessage = function(channel, msg){
  // TODO: only subscribe on {Queue,Job}#on()
  if ((msg) && (msg.length >1))
  {
    var msg = JSON.parse(msg[1]);

    // map to Job when in-process
    var job = exports.jobs[msg.id];
    if (job) {
      job.emit.apply(job, msg.args);

      // TODO: abstract this out
      if ('removed' === msg.event){
        delete exports.jobs[job.id];
      }
    }

    // emit args on Queues
    msg.args[0] = 'job ' + msg.args[0];
    msg.args.push(msg.id);
    exports.queue.emit.apply(exports.queue, msg.args);
  }
};

/**
 * Emit `event` for for job `id` with variable args.
 *
 * @param {Number} id
 * @param {String} event
 * @param {Mixed} ...
 * @api private
 */

exports.emit = function(id, event) {
  if (!pubClient){
    pubClient = redis.createClient();
  }

  var msg = JSON.stringify({
      id: id
    , event: event
    , args: [].slice.call(arguments, 1)
  });

  var multi = pubClient.multi();
  multi.rpush([getDefQueueKey(id), msg]);
  // get the registered clients for this event,
  multi.smembers([getSubsKey(id)]);

  multi.exec(function(err, results){
    //notify them all
    var members = results[1];
    for (var itr in members){
      pubClient.rpush([getClientSubKey(id,members[itr]), msg], function(err,results) {
        if (err) {
          console.log('Kue event error: ', err);
        }
      });
    }
  })

};
