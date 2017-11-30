

exports.callbackOrReturn = function(callback) {
  return function(result) {
    if (callback) return callback(null, result);
    return result;
  }
};

exports.callbackOrThrow = function(callback) {
  return function (err) {
    if (callback) return callback(err);
    throw err;
  }
};