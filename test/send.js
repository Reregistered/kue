/**
 * Created by dov on 1/21/14.
 */
/**
 * Created by dov on 1/21/14.
 */

var redis = require('redis'),
  qs = require('querystring'),
  guid = require('node-guid'),
  restify   = require('restify'),
  kue = require('../index.js');

var fn_null = function(){};

var interval = null;
var JOB_NAME = 'job_test';

////////////////////////////////////////
// setup the job queue we'll use
kue.redis.createClient = function() {
  var kue_client = redis.createClient(6379, '127.0.0.1');
  kue_client.on('error',function(err){
    winston.error('Kue, caught' + err.message);
  });

  return kue_client;
};

// list of currently running jobs.
var jobQueue = kue.createQueue();

function addJob(in_jobkey, in_jobData, in_callback){

  in_callback = in_callback || fn_null;
  console.log('creating: ' + in_jobData.time);
  var job = jobQueue.create(in_jobkey, in_jobData);
  var jobguid = guid.new();
  console.log('  guid: ' + jobguid);
  job.save({id:jobguid, expire: 60}, function(err,currentJob){
    in_callback(err, jobguid);
  });
}

function removeJob(in_jobId, in_callback) {
  in_callback = in_callback || fn_null;
  kue.Job.get(in_jobId, function(err, job){

    if (job){
      job.remove(function(){
        console.log('  removed: ' + job.data.time );
        in_callback(err, job);
      });
    }else{
      console.log('    running : false');
      in_callback(null, null);
    }
  });
}

function clearTimers(){
  if (interval) {
    clearInterval(interval);
    interval = null;
  }
}

function rte_start_test(req, res, next) {

  var urlParams = req.url.split('?');
  var params = {};
  if (urlParams.length >1){
    params = qs.parse(urlParams[1]);
  }

  var job_name = params.name || JOB_NAME;
  var addTime = Number(params.addTime) || 5000;
  var removeTime = Number(params.removeTime) || 3000;

  clearTimers();

  var testFunc = function(){
    addJob(job_name, {time:Date()}, function(err, jobId){
      setTimeout(function(){
        removeJob(jobId);
      }, removeTime);
    });
  };

  testFunc();
  interval = setInterval(testFunc, addTime);

  res.end('');
}

function rte_stop_test(req, res, next) {

  clearTimers();

  res.end('');
}

function rte_add_job(req, res, next) {
  try{
    var params = qs.parse(req._url.query);
  } catch(err){
    params = {};
  }
  var job_name = params.name || JOB_NAME;
  addJob(job_name, {time:Date()});
  res.end('');
}

function rte_remove_job(req, res, next) {
  try{
    var params = qs.parse(req._url.query);
  } catch(err){
    params = {};
  }
  var job_name = params.name || JOB_NAME;
  removeJob(job_name);
  res.end('');
}


var rest_port = 12031;
var myroutes = {
  post:{
    '/start/?' : rte_start_test,
    '/stop' : rte_stop_test,
    '/add/?' : rte_add_job,
    '/remove/?': rte_remove_job
  }
};

var rest_server = restify.createServer();
for (itr in myroutes.post){
  rest_server.post(itr, myroutes.post[itr])
}
rest_server.listen(rest_port, function() {
  console.log('%s listening on port %s', rest_server.name, rest_port);
});
