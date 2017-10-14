const flatten = require('../utils/flatten')

module.exports = ({ topics }) => {
  const partitions = topics.map(({ topicName, partitions }) =>
    partitions.map(partition => Object.assign({ topicName }, partition))
  )
  return flatten(partitions)
}
