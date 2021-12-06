const Runner = require('./runner')

/**
 * Pool of runners for consuming partitions concurrently.
 *
 * @param {object} options
 * @param {import('./consumerGroup')} options.consumerGroup
 * @param {(err: Error) => Promise<void>} options.onCrash
 * @param {number} options.partitionsConsumedConcurrently
 * @returns
 */
const createRunnerPool = ({
  autoCommit,
  logger,
  consumerGroup,
  instrumentationEmitter,
  eachBatchAutoResolve,
  eachBatch,
  eachMessage,
  heartbeatInterval,
  retry,
  onCrash,
  partitionsConsumedConcurrently,
}) => {
  /** @type {Runner[]} */
  let runners = []
  let running = false

  const start = async () => {
    if (running) return
    running = true

    const runnerIds = Array.from(Array(partitionsConsumedConcurrently).keys())

    runners = runnerIds.map(
      runnerId =>
        new Runner({
          runnerId,
          autoCommit,
          logger,
          consumerGroup,
          instrumentationEmitter,
          eachBatchAutoResolve,
          eachBatch,
          eachMessage,
          heartbeatInterval,
          retry,
          onCrash,
        })
    )

    await consumerGroup.connect()
    await consumerGroup.joinAndSync()

    await Promise.all(runners.map(r => r.start()))
  }

  const stop = async () => {
    if (!running) return
    running = false

    await Promise.all(runners.map(r => r.stop()))
    runners = []

    await consumerGroup.leave()
  }

  const commitOffsets = async offsets => {
    await Promise.all(runners.map(r => r.commitOffsets(offsets)))
  }

  return { start, stop, commitOffsets }
}

module.exports = createRunnerPool
