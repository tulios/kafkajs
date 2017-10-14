module.exports = ({ topic, partitions, messagesPerPartition }) => [
  {
    topic,
    partitions: partitions.map(partition => ({
      partition,
      messages: messagesPerPartition[partition],
    })),
  },
]
