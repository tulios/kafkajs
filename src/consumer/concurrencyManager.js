const { entries } = Object

/**
 * @param {{ concurrency: number }} options.concurrency
 */
const concurrencyManager = ({ concurrency }) => {
  /**
   * @type {{ [runnerId: number]: { [topic: string]: Set<number> } }}
   */
  const runnerTopicPartitions = Array.from(Array(concurrency).keys()).reduce(
    (acc, runnerId) => ({ ...acc, [runnerId]: {} }),
    {}
  )

  /**
   * Distribute partitions between the runners while trying to assign the same nodeIds to the same runnerIds.
   *
   * @param {object} options
   * @param {import("types").TopicPartitions[]} options.assignment
   * @param {(topic: string, partitions: number[]) => { [nodeId: number]: number[] }} options.findReadReplicaForPartitions
   */
  const assign = ({ assignment, findReadReplicaForPartitions }) => {
    // n - number of nodes
    // runnerId = n mod concurrency

    const nodeIds = []

    assignment.forEach(({ topic, partitions }) => {
      const nodePartitions = findReadReplicaForPartitions(topic, partitions)

      entries(nodePartitions).forEach(([nodeId, partitions]) => {
        let nodeIndex = nodeIds.indexOf(nodeId)
        if (nodeIndex === -1) {
          nodeIds.push(nodeId)
          nodeIndex = nodeIds.length - 1
        }

        const runnerId = nodeIndex % concurrency

        if (!runnerTopicPartitions[runnerId][topic])
          runnerTopicPartitions[runnerId][topic] = new Set()

        runnerTopicPartitions[runnerId][topic] = new Set([
          ...runnerTopicPartitions[runnerId][topic],
          ...partitions,
        ])
      })
    })
  }

  /**
   * @param {number} runnerId
   * @returns {(import("types").TopicPartitions[]) => import("types").TopicPartitions[]}
   */
  const filterConcurrent = runnerId => topicPartitions => {
    return topicPartitions
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter(p =>
          (runnerTopicPartitions[runnerId][topic] || new Set()).has(p)
        ),
      }))
      .filter(({ partitions }) => partitions.length)
  }

  return { assign, filterConcurrent }
}

module.exports = concurrencyManager
