exports.index = async function (collection) {
  // Ensures there's a reasonable index for the poling dequeue
  // Status is first b/c querying by status = queued should be very selective
  try {
    await collection.createIndex({
      status: 1,
      queue: 1,
      priority: 1,
      _id: 1,
      delay: 1,
    });
  } catch (err) {
    if (err) console.error(err);
  }
};
