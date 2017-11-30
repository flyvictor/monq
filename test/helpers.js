var async = require('async');
const MongoClient = require('mongodb').MongoClient;

exports.uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/monq_tests';

exports.db = MongoClient.connect(exports.uri);

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