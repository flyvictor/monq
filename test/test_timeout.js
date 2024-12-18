var assert = require('assert');
var helpers = require('./helpers');
var Queue = require('../lib/queue');
var sinon = require('sinon');
var Worker = require('../lib/worker');

describe('Timeout', function () {
    var queue, handler, worker, failed;

    beforeEach(function () {
        queue = new Queue({ _ready: helpers.db });

        handler = sinon.spy(function (params, callback) {
            // Don't call the callback, let it timeout
        });

        failed = sinon.spy();

        worker = new Worker([queue], { interval: 10 });
        worker.register({ timeout: handler });
        worker.on('failed', failed);
    });


    afterEach(async function () {
        const collection = await queue.collection;
        await collection.deleteMany({});
    });

    describe('worker processing job with a timeout', function () {
        beforeEach(function (done) {
            queue.enqueue('timeout', {}, { timeout: 10 }, done);
        });

        beforeEach(function () {
            return helpers.flushWorker(worker);
        });

        it('calls the handler once', function () {
            assert.equal(handler.callCount, 1);
        });

        it('emits `failed` event once', function () {
            assert.equal(failed.callCount, 1);
        });

        it('updates the job status', function () {
            var job = failed.lastCall.args[0];

            assert.equal(job.status, 'failed');
            assert.equal(job.error, 'timeout');
        });
    });

    describe('worker processing job with a timeout and retries', function () {
        beforeEach(function (done) {
            queue.enqueue('timeout', {}, { timeout: 10, attempts: { count: 3 }}, done);
        });

        beforeEach(function () {
            return helpers.flushWorker(worker);
        });

        it('calls the handler three times', function () {
            assert.equal(handler.callCount, 3);
        });

        it('updates the job status', function () {
            var job = failed.lastCall.args[0];

            assert.equal(job.status, 'failed');
            assert.equal(job.error, 'timeout');
        });
    });
});