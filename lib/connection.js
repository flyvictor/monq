const MongoClient = require('mongodb').MongoClient;
const Queue = require('./queue');
const Worker = require('./worker');

module.exports = Connection;

function Connection(uri, options) {
  this.db = this._ready = MongoClient.connect(uri, options)
    .then(db => {
      return db;
    })
    .catch(err => {
      throw err;
    });
}

Connection.prototype.worker = function (queues, options) {
  const ques = queues.map(queue => {
    if (typeof queue === 'string') {
      queue = this.queue(queue);
    }

    return queue;
  });

  return new Worker(ques, options);
};

Connection.prototype.queue = function (name, options) {
  return new Queue(this, name, options);
};

Connection.prototype.close = function () {
  this._ready.then(db =>
    db.close()
  );
};