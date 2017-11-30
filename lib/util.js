

exports.callbackOrReturn = callback => result => {
  if (callback) return callback(null, result);
  return result;
};

exports.callbackOrThrow = callback => err => {
  if (callback) return callback(err);
  throw err;
};