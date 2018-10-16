const flatten = require('../utils/flatten')

module.exports = ({ topics }) => {
  const partitions = topics.map(({ topicName, partitions }) =>
    partitions.map(partition => ({ topicName, ...partition }))
  )

  return flatten(partitions)
}
