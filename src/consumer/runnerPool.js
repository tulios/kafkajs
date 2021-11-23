const Runner = require('./runner')

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

  const createRunnerIds = () => Array.from(Array(partitionsConsumedConcurrently).keys())

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

module.exports = createRunnerPool
