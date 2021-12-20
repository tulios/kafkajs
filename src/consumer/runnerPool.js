const { isRebalancing } = require('../errors')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const Runner = require('./runner')
const {
  events: { REBALANCING },
} = require('./instrumentationEvents')

const createRunnerPool = ({
  autoCommit,
  logger: rootLogger,
  consumerGroup,
  instrumentationEmitter,
  eachBatchAutoResolve,
  eachBatch,
  eachMessage,
  heartbeatInterval,
  retry,
  onCrash,
  concurrency,
}) => {
  const logger = rootLogger.namespace('RunnerPool')

  let runners = []
  let running = false

  const start = async () => {
    if (running) return
    running = true

    try {
      await consumerGroup.connect()
      await consumerGroup.joinAndSync()
    } catch (error) {
      return onCrash(error)
    }

    runners = Array.from(Array(concurrency).keys()).map(
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
        })
    )

    return startRunners()
  }

  const stop = async () => {
    if (!running) return
    running = false

    await stopRunners()
    runners = []

    try {
      await consumerGroup.leave()
    } catch (e) {}
  }

  const startRunners = () => Promise.all(runners.map(r => r.start({ recover })))

  const stopRunners = () => Promise.all(runners.map(r => r.stop()))

  const commitOffsets = async offsets => {
    const { groupId, memberId } = consumerGroup

    const [runner] = runners
    if (!runner) {
      logger.debug('consumer not running, exiting', { groupId, memberId, offsets })
      return
    }

    try {
      return await runner.commitOffsets(offsets)
    } catch (error) {
      await recover(error)
      throw error
    }
  }

  const recover = sharedPromiseTo(async error => {
    await stopRunners()

    const { groupId, memberId } = consumerGroup

    try {
      if (isRebalancing(error)) {
        logger.error('The group is rebalancing, re-joining', {
          groupId,
          memberId,
          error: error.message,
        })

        instrumentationEmitter.emit(REBALANCING, { groupId, memberId })

        await consumerGroup.joinAndSync()

        if (running) startRunners()
        return
      }

      if (error.type === 'UNKNOWN_MEMBER_ID') {
        logger.error('The coordinator is not aware of this member, re-joining the group', {
          groupId,
          memberId,
          error: error.message,
        })

        consumerGroup.memberId = null
        await consumerGroup.joinAndSync()

        if (running) startRunners()
        return
      }
    } catch (e) {
      onCrash(e)
      return
    }

    onCrash(error)
  })

  return { start, stop, commitOffsets }
}

module.exports = createRunnerPool
