const Runner = require('./runner')

const runnerPool = ({
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
  partitionsConsumedConcurrently = 1,
}) => {
  /** @type {Runner[]} */
  let runners = []

  const createRunnerIds = () => Array.from(Array(1).keys()) // TODO: Replace with partitionsConsumedConcurrently

  const start = async () => {
    const runnerIds = createRunnerIds()

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
          partitionsConsumedConcurrently,
        })
    )

    await Promise.all(runners.map(r => r.start()))
  }

  const stop = async () => {
    await Promise.all(runners.map(r => r.stop()))

    runners = []
  }

  const commitOffsets = async offsets => {
    await Promise.all(runners.map(r => r.commitOffsets(offsets)))
  }

  return { start, stop, commitOffsets }
}

module.exports = runnerPool
