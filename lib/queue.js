var ObjectId = require('mongodb').ObjectId;
var db = require('./db');
var Job = require('./job');
var util = require('./util');

module.exports = Queue;

function Queue(connection, name, options) {
  if (typeof name === 'object' && options === undefined) {
    options = name;
    name = undefined;
  }

  options || (options = {});
  options.collection || (options.collection = 'jobs');

  var self = this;
  this.name = name || 'default';
  this.options = options;

  this.collection = connection._ready.then(function(db) {
    return db.collection(self.options.collection);
  });
  this.collection.then(function(collection) {
    if (options.index !== false) {
      db.index(collection);
    }
  });
}

Queue.prototype.job = function (data) {
  return new Job(this.collection, data);
};

Queue.prototype.get = function (id, callback) {
  if (typeof id === 'string') {
    id = new ObjectId(id);
  }
  var self = this;

  return this.collection
    .then(function(collection) { return collection.findOne({ _id: id, queue: self.name })})
    .then(function(jobData) { return self.job(jobData)})
    .then(util.callbackOrReturn(callback))
    .catch(util.callbackOrThrow(callback));
};

Queue.prototype.enqueue = function (name, params, options, callback) {
  if (!callback && typeof options === 'function') {
    callback = options;
    options = {};
  }

  var job = this.job({
    name: name,
    params: params,
    queue: this.name,
    attempts: parseAttempts(options.attempts),
    timeout: parseTimeout(options.timeout),
    delay: options.delay,
    priority: options.priority
  });

  return job.enqueue(callback);
};

Queue.prototype.dequeue = function (options, callback) {
  var self = this;

  if (callback === undefined) {
    callback = options;
    options = {};
  }

  var query = {
    status: Job.QUEUED,
    queue: {$regex: '^' + this.name + '$'},
    delay: { $lte: new Date() }
  };

  if (options.minPriority !== undefined) {
    query.priority = { $gte: options.minPriority };
  }

  if (options.callbacks !== undefined) {
    var callback_names = Object.keys(options.callbacks);
    query.name = { $in: callback_names };
  }

  var sort = {
    priority: -1,
    delay: 1
  };
  var update = { $set: { status: Job.DEQUEUED, dequeued: new Date() }};

  return this.collection
    .then(function(collection) {
      return collection.findOneAndUpdate(query, update, {
        sort: sort,
        returnDocument: 'after'
      })
    })
    .then(function(doc) { return doc && doc.value && self.job(doc.value)})
    .then(util.callbackOrReturn(callback))
    .catch(util.callbackOrThrow(callback));
};

// Helpers

function parseTimeout(timeout) {
  if (timeout === undefined) return undefined;
  return parseInt(timeout, 10);
}

function parseAttempts(attempts) {
  if (attempts === undefined) return undefined;

  if (typeof attempts !== 'object') {
    throw new Error('attempts must be an object');
  }

  var result = {
    count: parseInt(attempts.count, 10)
  };

  if (attempts.delay !== undefined) {
    result.delay = parseInt(attempts.delay, 10);
    result.strategy = attempts.strategy;
  }

  return result;
}
