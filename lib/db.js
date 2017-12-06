exports.index = function (collection) {
    // Ensures there's a reasonable index for the poling dequeue
    // Status is first b/c querying by status = queued should be very selective
    collection.ensureIndex({ status: 1, queue: 1, priority: 1, _id: 1, delay: 1 }, function (err) {
        if (err) console.error(err);
    });
};