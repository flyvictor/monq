'use strict';
var assert = require('assert');
var Profiler = require('../lib/profiler');

describe('Profiler', function(){
  it('should log stages', function(){
    var prof = new Profiler();
    prof.start('stage1');
    prof.end('stage1');
    assert.ok(prof.log.stage1);
  });
  it('should provide meaningful stats', function(done){
    var prof = new Profiler();
    prof.start('stage1');
    setTimeout(function(){
      prof.end('stage1');
      var stats = prof.getStats();
      assert.ok(stats.stage1.ms >= 50);
      assert.ok(stats.stage1.startedAt < stats.stage1.endedAt);
      done();
    }, 50);
  });
});


