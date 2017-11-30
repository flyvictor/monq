const ObjectId = require('mongodb').ObjectId;
const db = require('./db');
const Job = require('./job');
const util = require('./util');

module.exports = Queue;

function Queue(connection, name, options) {
  if (typeof name === 'object' && options === undefined) {
    options = name;
    name = undefined;
  }

  options || (options = {});
  options.collection || (options.collection = 'jobs');

  this.name = name || 'default';
  this.options = options;

  this.collection = connection._ready.then(db => db.collection(this.options.collection));
  this.collection.then(collection => {
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
  return this.collection
    .then(collection => collection.findOne({ _id: id, queue: this.name }))
    .then(jobData => this.job(jobData))
    .then(util.callbackOrReturn(callback))
    .catch(util.callbackOrThrow(callback));
};

Queue.prototype.enqueue = function (name, params, options, callback) {
  if (!callback && typeof options === 'function') {
    callback = options;
    options = {};
  }

  const job = this.job({
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

  if (callback === undefined) {
    callback = options;
    options = {};
  }

  const query = {
    status: Job.QUEUED,
    queue: {$regex: '^' + this.name + '$'},
    delay: { $lte: new Date() }
  };

  if (options.minPriority !== undefined) {
    query.priority = { $gte: options.minPriority };
  }

  if (options.callbacks !== undefined) {
    const callback_names = Object.keys(options.callbacks);
    query.name = { $in: callback_names };
  }

  const sort = {
    priority: -1,
    _id: 1
  };
  const update = { $set: { status: Job.DEQUEUED, dequeued: new Date() }};

  return this.collection
    .then(collection => collection.findOneAndUpdate(query, update, {
      sort: sort,
      returnOriginal: false
    }))
    .then(doc => doc && doc.value && this.job(doc.value))
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

  const result = {
    count: parseInt(attempts.count, 10)
  };

  if (attempts.delay !== undefined) {
    result.delay = parseInt(attempts.delay, 10);
    result.strategy = attempts.strategy;
  }

  return result;
}
