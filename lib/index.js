var Connection = require('./connection');

module.exports = function (uri, dbName, options) {
    return new Connection(uri, dbName, options);
};
