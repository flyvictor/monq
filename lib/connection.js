var MongoClient = require('mongodb').MongoClient;
var Queue = require('./queue');
var Worker = require('./worker');

module.exports = Connection;

function Connection(uri, options) {
  this.db = this._ready = MongoClient.connect(uri, options)
    .then(function(db) {
      return db;
    })
    .catch(function(err) {
      throw err;
    });
}

Connection.prototype.worker = function (queues, options) {
  var self = this;
  var ques = queues.map(function(queue) {
    if (typeof queue === 'string') {
      queue = self.queue(queue);
    }

    return queue;
  });

  return new Worker(ques, options);
};

Connection.prototype.queue = function (name, options) {
  return new Queue(this, name, options);
};

Connection.prototype.close = function () {
  return this._ready.then(function(db) {
    db.close();
  });
};
