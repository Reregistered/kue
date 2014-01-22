/**
 * Created by dov on 1/21/14.
 */
/**
 * Created by dov on 1/21/14.
 */

var redis = require('redis'),
  kue = require('../index.js');

var fn_null = function(){};

////////////////////////////////////////
// setup the job queue we'll use
kue.redis.createClient = function() {
  var kue_client = redis.createClient(6379, '127.0.0.1');
  kue_client.on('error',function(err){
    console.log('Kue, caught' + err.message);
  });

  return kue_client;
};

// list of currently running jobs.
var jobQueue = kue.createQueue();

// register a job handler
jobQueue.process('job_test', function(job, jobCallback){

  console.log('received: ' + job.data.time);
  job.on('removed', function(){
    console.log('  removed: ' + job.data.time);
    jobCallback(null,true);
  })

});
