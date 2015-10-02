'use strict';

function Profiler(job){
  this.job = job;
  this.log = {};
  this.pendingSteps = [];
}

Profiler.prototype.start = function(name){
  if (this.log[name]) console.warn('Stage name ', name, ' is already in use');
  this.log[name] = {startedAt: Date.now(), endedAt: null};
  this.pendingSteps.push(name);
};

Profiler.prototype.end = function(name){
  if (!this.log[name]) return console.error('Stage ', name, ' has not started yet');
  this.log[name].endedAt = Date.now();
  this.pendingSteps.splice(this.pendingSteps.indexOf(name), 1);
};

Profiler.prototype.endAll = function(){
  var that = this;
  that.pendingSteps.forEach(function(step){
    that.end(step);
  });
};

Profiler.prototype.getStats = function(){
  var that = this;
  return Object.keys(this.log).reduce(function(memo, key){
    memo[key] = {
      startedAt: that.log[key].startedAt,
      endedAt: that.log[key].endedAt,
      ms: that.log[key].endedAt - that.log[key].startedAt
    };
    return memo;
  }, {});
};

module.exports = Profiler;