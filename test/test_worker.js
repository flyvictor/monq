var assert = require('assert');
var sinon = require('sinon');
var Worker = require('../lib/worker');

describe('Worker', function () {
    var job, queues, worker;

    beforeEach(function () {
        job = {
            data: {},
            complete: function () {},
            fail: function () {}
        };

        queues = ['foo', 'bar', 'baz'].map(function (name) {
            return {
                enqueue: function () {},
                dequeue: function () {}
            };
        });

        worker = new Worker(queues);
    });

    it('has default polling interval', function () {
        assert.equal(worker.interval, 5000);
    });

    it('is an event emitter', function (done) {
        worker.on('foo', function (bar) {
            assert.equal(bar, 'bar');
            done();
        });

        worker.emit('foo', 'bar');
    });

    describe('when dequeuing', function () {
        it('cycles queues', function () {
            var foo = sinon.stub(worker.queues[0], 'dequeue').yields();
            var bar = sinon.stub(worker.queues[1], 'dequeue').yields();
            var baz = sinon.stub(worker.queues[2], 'dequeue').yields();

            worker.dequeue(function () {});
            worker.dequeue(function () {});
            worker.dequeue(function () {});
            worker.dequeue(function () {});

            assert.ok(foo.calledTwice);
            assert.ok(foo.calledBefore(bar));
            assert.ok(bar.calledOnce);
            assert.ok(bar.calledBefore(baz));
            assert.ok(baz.calledOnce);
            assert.ok(baz.calledBefore(foo));
        });
    });

    describe('when polling', function () {
        describe('when error', function () {
            it('emits an `error` event', function (done) {
                var error = new Error();

                sinon.stub(worker, 'dequeue').yields(error);

                worker.on('error', function (err) {
                    assert.equal(err, error);
                    done();
                });

                worker.start();
            });
            it('handles `collection going away` error', function (done) {
                var error = new Error('Operation aborted: collection going away');
                var error2 = new Error('different error');

                sinon.stub(worker, 'dequeue')
                    .onFirstCall().yields(error)
                    .onSecondCall().yields(error2);

                worker.on('error', function (err) {
                    assert.equal(err.message, 'different error');
                    done();
                });

                worker.start();
            });
        });

        describe('when job is available', function () {
            var work;

            beforeEach(function () {
                work = sinon.stub(worker, 'work');

                worker.queues = [worker.queues[0]]; //We don't need three queues here

                sinon.stub(worker.queues[0], 'dequeue')
                  .onFirstCall().yields(null, job)
                  .onSecondCall().yields(null, {data: "second run"})
                  .onThirdCall().yields(null, null);
            });

            afterEach(function(){
              worker.work.restore();
            });

            it('works on the job', function () {
                worker.start();

                assert.ok(work.calledOnce);
                assert.equal(work.getCall(0).args[0], job);
            });

            it('emits `dequeued` event', function (done) {
                worker.on('dequeued', function (j) {
                    assert.equal(j, job.data);
                    done();
                });

                worker.start();
            });
            it('does not wait for job to complete when explicitly told to', function(done){
              worker.parallel = true;
              worker.start();

              worker.on('empty', function(){
                assert.ok(work.calledTwice);
                done();
              });
            });

            it('does not break the order of jobs running in parallel mode', function(done){
              worker.parallel = true;
              worker.start();

              worker.on('empty', function(){
                assert.equal(work.getCall(0).args[0], job);
                assert.deepEqual(work.getCall(1).args[0], {data: "second run"});
                done();
              });
            });
        });

        describe('when no job is available', function () {
            var clock;

            beforeEach(function () {
                clock = sinon.useFakeTimers();

                sinon.stub(worker.queues[0], 'dequeue').yields(null, null);
                sinon.stub(worker.queues[1], 'dequeue').yields(null, null);
                sinon.stub(worker.queues[2], 'dequeue').yields(null, null);
            });

            afterEach(function () {
                clock.restore();
            });

            it('waits an interval before polling again', function () {
                worker.start();

                var poll = sinon.spy(worker, 'poll');
                clock.tick(worker.interval);
                worker.stop();

                assert.ok(poll.calledOnce);
            });
        });

        describe('when stopping with a job in progress', function () {
            var dequeueStubs;

            beforeEach(function () {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, job);
                });

                sinon.stub(worker, 'process').yields(null, 'foobar');
                sinon.stub(job, 'complete').yields();

                worker.start();
                worker.work(job);
            });

            it('waits for the job to finish', function (done) {
                assert.ok(worker.working);

                worker.stop(function () {
                    assert.ok(!worker.working);
                    assert.ok(dequeueStubs[0].calledOnce);

                    // It doesn't get the stop signal until after the next dequeue is in motion
                    assert.ok(dequeueStubs[1].calledOnce);

                    // Make sure it didn't continue polling after we told it to stop
                    assert.ok(!dequeueStubs[2].called);

                    assert.equal(worker.listeners('done').length, 0);
                    done();
                });
            });
        });

        describe('when stopping during an empty dequeue', function () {
            var dequeueStubs;

            beforeEach(function () {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start();
            });

            it('stops cleanly', function (done) {
                assert.ok(worker.working);

                worker.stop(function () {
                    assert.ok(!worker.working);
                    assert.ok(dequeueStubs[0].called);

                    // Make sure it didn't continue polling after we told it to stop
                    assert.ok(!dequeueStubs[1].called);

                    assert.ok(!dequeueStubs[2].called);
                    assert.equal(worker.listeners('done').length, 0);
                    done();
                });
            });
        });

        describe('when stopping between polls', function () {
            var dequeueStubs;

            beforeEach(function () {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start();
            });

            it('stops cleanly', function (done) {
                assert.ok(worker.working);

                worker.once('empty', function () {
                    worker.stop(function () {
                        assert.ok(!worker.working);
                        assert.ok(dequeueStubs[0].called);

                        // Make sure it didn't continue polling after we told it to stop
                        assert.ok(!dequeueStubs[1].called);

                        assert.ok(!dequeueStubs[2].called);
                        assert.equal(worker.listeners('done').length, 0);
                        done();
                    });
                });
            });
        });

        describe('when stopping twice', function () {
            var dequeueStubs;

            beforeEach(function () {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start();
            });

            it('does not error', function (done) {
                worker.stop(function () {
                    worker.stop();
                    done();
                });
            });
        });
    });

    describe('when working', function () {
        describe('when processing fails', function () {
            var error, fail, poll;

            beforeEach(function () {
                error = new Error();

                fail = sinon.stub(job, 'fail').yields();
                poll = sinon.spy(worker, 'poll');

                sinon.stub(worker, 'process').yields(error);
            });

            it('fails the job', function () {
                worker.work(job);

                assert.ok(fail.calledOnce);
                assert.equal(fail.getCall(0).args[0], error)
            });

            it('emits `done` event', function (done) {
                worker.on('done', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('emits `failed` event', function (done) {
                worker.on('failed', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('polls for a new job', function () {
                worker.work(job);

                assert.ok(poll.calledOnce);
            });
        });

        describe('when processing succeeds', function () {
            var complete, poll;

            beforeEach(function () {
                complete = sinon.stub(job, 'complete').yields();
                poll = sinon.spy(worker, 'poll');

                sinon.stub(worker, 'process').yields(null, 'foobar');
            });

            it('completes the job', function () {
                worker.work(job);

                assert.ok(complete.calledOnce);
                assert.equal(complete.getCall(0).args[0], 'foobar')
            });

            it('emits `done` event', function (done) {
                worker.on('done', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('emits `complete` event', function (done) {
                worker.on('complete', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('polls for a new job', function () {
                worker.work(job);

                assert.ok(poll.calledOnce);
            });
        });
    });

    describe('when processing', function () {
        beforeEach(function () {
            worker.register({
                example: function (params, callback) {
                    callback(null, params);
                }
            });
        });

        it('passes job to registered callback', function (done) {
            worker.process({}, { name: 'example', params: { foo: 'bar' }}, function (err, result) {
                assert.deepEqual(result, { foo: 'bar' });
                done();
            });
        });

        it('returns error if there is no registered callback', function (done) {
            worker.process({}, { name: 'asdf' }, function (err, result) {
                assert.ok(err);
                done();
            });
        });
    });
});