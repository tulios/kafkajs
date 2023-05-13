module.exports = ({ topic, partitionMetadata, messages, partitioner }) => {
  if (partitionMetadata.length === 0) {
    return {}
  }

  const grouping = {}
  for (const message of messages) {
    const partition = partitioner({ topic, partitionMetadata, message })
    if (grouping[partition] === undefined) {
      grouping[partition] = []
    }
    grouping[partition].push(message)
  }

  return grouping
}
