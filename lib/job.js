var events = require('events');
var ObjectId = require('mongodb').ObjectId;
var utils = require('./util');
var util = require('util');

module.exports = Job;

function Job(collection, data) {
    this.collection = collection;

    if (data) {
        // Convert plain object to JobData type
        data.__proto__ = JobData.prototype;
        this.data = data;
    } else {
        this.data = new JobData();
    }
}

util.inherits(Job, events.EventEmitter);

Job.QUEUED = 'queued';
Job.DEQUEUED = 'dequeued';
Job.COMPLETE = 'complete';
Job.FAILED = 'failed';
Job.CANCELLED = 'cancelled';

Job.prototype.save = function (callback) {
    var self = this;

    if (this.data._id === undefined){
        this.data._id = new ObjectId();
    }

    var query = {
        _id: this.data._id
    };
    var options = {
        upsert: true
    };

    return this.collection
      .then(function(collection){ return collection.replaceOne(query, self.data, options)})
      .then(function() {return self}) //.save returns Job instance
      .then(utils.callbackOrReturn(callback))
      .catch(utils.callbackOrThrow(callback));
};

Job.prototype.cancel = function (callback) {
    if (this.data.status !== Job.QUEUED) {
        return callback(new Error('Only queued jobs may be cancelled'));
    }

    this.data.status = Job.CANCELLED;
    this.data.ended = new Date();

    return this.save(callback);
};

Job.prototype.complete = function (result, callback) {
    this.data.status = Job.COMPLETE;
    this.data.ended = new Date();
    this.data.result = result;

    return this.save(callback);
};

Job.prototype.fail = function (err, callback) {
    this.data.status = Job.FAILED;
    this.data.ended = new Date();
    this.data.error = err.message;
    this.data.stack = err.stack;

    return this.save(callback);
};

Job.prototype.enqueue = function (callback) {
    if (this.data.delay === undefined) {
        this.data.delay = new Date();
    }

    if (this.data.priority === undefined) {
        this.data.priority = 0;
    }

    this.data.status = Job.QUEUED;
    this.data.enqueued = this.data.enqueued || new Date();

    return this.save(callback);
};

Job.prototype.delay = function (delay, callback) {
    this.data.delay = new Date(new Date().getTime() + delay);

    return this.enqueue(callback);
};

function JobData() {}

Object.defineProperty(JobData.prototype, 'id', {
    get: function () {
        return this._id && this._id.toString && this._id.toString();
    }
});
