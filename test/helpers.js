var async = require('async');
var MongoClient = require('mongodb').MongoClient;

exports.uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/monq_tests';
exports.dbName = process.env.MONGODB_NAME|| 'monq_tests';

exports.db = MongoClient.connect(exports.uri).then(connection => connection.db(exports.dbName));

exports.each = function (fixture, fn, done) {
    async.each(fixture, function (args, callback) {
        fn.apply(undefined, args.concat([callback]));
    }, done);
};

exports.flushWorker = function (worker, done) {
    worker.start();
    worker.once('empty', function () {
        worker.stop(done);
    });
};
