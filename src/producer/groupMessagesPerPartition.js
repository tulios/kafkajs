module.exports = ({ topic, partitionMetadata, messages, partitioner }) => {
  return messages.reduce((result, message) => {
    const partition = partitioner({ topic, partitionMetadata, message })
    const current = result[partition] || []
    return Object.assign(result, { [partition]: [...current, message] })
  }, {})
}
