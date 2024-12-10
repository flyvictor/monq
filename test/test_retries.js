var assert = require('assert');
var sinon = require('sinon');
var helpers = require('./helpers');
var Job = require('../lib/job');
var Queue = require('../lib/queue');
var Worker = require('../lib/worker');

describe('Retries', function () {
    var queue, handler, worker, failed;

    beforeEach(function () {
        queue = new Queue({ _ready: helpers.db });

        handler = sinon.spy(function (params, callback) {
            return callback(new Error());
        });

        failed = sinon.spy();

        worker = new Worker([queue], {
            interval: 10,
            strategies: {
                predicate: function(attempts, error, jobdata) {
                    return jobdata.params.retry === true ? 0 : null;
                }
            }
        });
        worker.register({ retry: handler, predicate: handler });
        worker.on('failed', failed);
    });

    afterEach(async function () {
        const collection = await queue.collection;
        await collection.deleteMany({});
    });

    describe('worker retrying job', function () {
        beforeEach(function (done) {
            queue.enqueue('retry', {}, { attempts: { count: 3 } }, done);
        });

        beforeEach(function () {
            return helpers.flushWorker(worker);
        });

        it('calls the handler once for each retry', function () {
            assert.equal(handler.callCount, 3);
        });

        it('emits failed once for each failed attempt', function () {
            assert.equal(failed.callCount, 3);
        });

        it('updates the job status', function () {
            var job = failed.lastCall.args[0];

            assert.equal(job.attempts.remaining, 0);
            assert.equal(job.attempts.count, 3);
            assert.equal(job.status, 'failed');
        });
    });

    describe('retry predicate', function(){

        it('should retry the job if predicate matched', function(done){
           queue.enqueue('predicate', {retry: true }, {attempts: {count: 2, strategy: 'predicate'}}, async function(){
                await helpers.flushWorker(worker)
                const job = failed.lastCall.args[0];
                assert.equal(handler.callCount, 2);
                assert.equal(job.attempts.remaining, 0);
                assert.equal(job.attempts.count, 2);
                done();
           });
        });
        it('should not retry the job if predicate returns false', function(done){
            queue.enqueue('predicate', {retry: false }, {attempts: {count: 2, delay: 0, strategy: 'predicate'}}, async function(){
                await helpers.flushWorker(worker)
                const job = failed.lastCall.args[0];
                assert.equal(handler.callCount, 1);
                assert.equal(job.attempts.remaining, 1);
                assert.equal(job.attempts.count, 2);
                assert.equal(job.status, 'failed');
                done();
            });
        });
    });

    describe('worker retrying job with delay', function () {
        var start;

        beforeEach(function (done) {
            queue.enqueue('retry', {}, { attempts: { count: 3, delay: 100 } }, done);
        });

        describe('after first attempt', function () {
            beforeEach(function () {
                start = new Date();
                return helpers.flushWorker(worker);
            });

            it('calls handler once', function () {
                assert.equal(handler.callCount, 1);
            });

            it('emits `failed` once', function () {
                assert.equal(failed.callCount, 1);
            });

            it('re-enqueues job with delay', function () {
                var data = failed.lastCall.args[0];
                assert.equal(data.status, 'queued');
                assert.ok(new Date(data.delay).getTime() >= start.getTime() + 100);
            });

            it('does not immediately dequeue job', async function () {
                await helpers.flushWorker(worker);
                assert.equal(handler.callCount, 1)
            });
        });

        describe('after all attempts', function () {
            var delay;

            beforeEach(function () {
                delay = sinon.stub(Job.prototype, 'delay').callsFake(function (delay, callback) {
                    assert.equal(delay, 100);

                    this.data.delay = new Date();
                    this.enqueue(callback);
                });
            });

            beforeEach(function () {
                return helpers.flushWorker(worker);
            });

            afterEach(function () {
                delay.restore();
            });

            it('calls the handler once for each retry', function () {
                assert.equal(handler.callCount, 3);
            });

            it('emits failed once for each failed attempt', function () {
                assert.equal(failed.callCount, 3);
            });

            it('updates the job status', function () {
                var data = failed.lastCall.args[0];

                assert.equal(data.attempts.remaining, 0);
                assert.equal(data.attempts.count, 3);
                assert.equal(data.status, 'failed');
            });
        });
    });

    describe('worker retrying job with no retries', function () {
        beforeEach(function (done) {
            queue.enqueue('retry', {}, { attempts: { count: 0 }}, done);
        });

        beforeEach(function () {
            return helpers.flushWorker(worker);
        });

        it('calls the handler once', function () {
            assert.equal(handler.callCount, 1);
        });

        it('emits failed once', function () {
            assert.equal(failed.callCount, 1);
        });

        it('updates the job status', function () {
            var data = failed.lastCall.args[0];

            assert.equal(data.status, 'failed');
        });
    });
});