const { isRebalancing } = require('../errors')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const Runner = require('./runner')
const createRetry = require('../retry')
const {
  events: { REBALANCING },
} = require('./instrumentationEvents')

/**
 * @param {{
 *  autoCommit: boolean,
 *  logger: import('types').Logger,
 *  consumerGroup: import('./consumerGroup'),
 *  instrumentationEmitter: import('../instrumentation/emitter'),
 *  eachBatchAutoResolve: boolean,
 *  eachBatch: import('types').EachBatchHandler,
 *  eachMessage: import('types').EachMessageHandler,
 *  heartbeatInterval: number,
 *  retry: import('types').RetryOptions,
 *  onCrash: (e: Error) => void,
 *  concurrency: number,
 * }} options
 */
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
  const retrier = createRetry(Object.assign({}, retry))

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
          commitOffsetsIfNecessary: () => consumerGroup.commitOffsetsIfNecessary(),
          uncommittedOffsets: () => consumerGroup.uncommittedOffsets(),
          commitOffsets: offsets => consumerGroup.commitOffsets(offsets),
          hasSeekOffset: topicPartition => consumerGroup.hasSeekOffset(topicPartition),
          resolveOffset: offset => consumerGroup.resolveOffset(offset),
          heartbeat: () => consumerGroup.heartbeat({ interval: heartbeatInterval }),
          nextBatch: callback => consumerGroup.nextBatch(runnerId, callback),
          logger: logger.namespace(`Runner ${runnerId}`),
          instrumentationEmitter,
          eachBatchAutoResolve,
          autoCommit,
          retry,
          eachBatch,
          eachMessage,
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

  const startRunners = () => Promise.all(runners.map(r => r.start(recover)))

  const stopRunners = () => Promise.all(runners.map(r => r.stop()))

  const commitOffsets = async offsets => {
    const { groupId, memberId } = consumerGroup

    if (!running) {
      logger.debug('consumer not running, exiting', { groupId, memberId, offsets })
      return
    }

    try {
      return await retrier(async (_, retryCount, retryTime) => {
        try {
          await consumerGroup.commitOffsets(offsets)
        } catch (e) {
          const { message: error, stack, retriable } = e

          if (!running) {
            logger.debug('consumer not running, exiting', { error, groupId, memberId, offsets })
            return
          }

          if (retriable) {
            logger.debug('Error while committing offsets, trying again...', {
              groupId,
              memberId,
              error,
              stack,
              retryCount,
              retryTime,
              offsets,
            })
          }
          throw e
        }
      })
    } catch (e) {
      await recover(e)
      throw e
    }
  }

  const recover = sharedPromiseTo(async error => {
    await stopRunners()

    const { groupId, memberId } = consumerGroup
    const { originalError = error } = error

    try {
      if (isRebalancing(originalError)) {
        logger.error('The group is rebalancing, re-joining', {
          groupId,
          memberId,
          error: originalError.message,
        })

        instrumentationEmitter.emit(REBALANCING, { groupId, memberId })

        await consumerGroup.joinAndSync()

        if (running) startRunners()
        return
      }

      if (originalError.type === 'UNKNOWN_MEMBER_ID') {
        logger.error('The coordinator is not aware of this member, re-joining the group', {
          groupId,
          memberId,
          error: originalError.message,
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
