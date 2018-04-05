const createRetry = require('../retry')
const { KafkaJSError } = require('../errors')

const isTestMode = process.env.NODE_ENV === 'test'

const isRebalancing = e =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP'

const isKafkaJSError = e => e instanceof KafkaJSError

module.exports = class Runner {
  constructor({
    consumerGroup,
    logger,
    eachBatchAutoResolve = true,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    onCrash,
    retry,
  }) {
    this.consumerGroup = consumerGroup
    this.logger = logger.namespace('Runner')
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    this.retrier = createRetry(Object.assign({}, retry))
    this.onCrash = onCrash

    this.running = false
    this.consuming = false
  }

  async join() {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.consumerGroup.join()
        await this.consumerGroup.sync()
        this.logger.info('Consumer has joined the group', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          leaderId: this.consumerGroup.leaderId,
        })
      } catch (e) {
        if (isRebalancing(e)) {
          // Rebalance in progress isn't a retriable error since the consumer
          // has to go through find coordinator and join again before it can
          // actually retry. Throwing a retriable error to allow the retrier
          // to keep going
          throw new KafkaJSError('The group is rebalancing')
        }

        bail(e)
      }
    })
  }

  async start() {
    if (this.running) {
      return
    }

    try {
      await this.join()

      this.running = true
      this.scheduleFetch()
    } catch (e) {
      this.onCrash(e)
    }
  }

  async stop() {
    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })
    this.running = false

    try {
      if (!isTestMode) {
        await this.waitForConsumer()
      }
      await this.consumerGroup.leave()
    } catch (e) {}
  }

  waitForConsumer() {
    return new Promise(resolve => {
      const scheduleWait = () => {
        this.logger.debug('waiting for consumer to finish...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
        })

        setTimeout(() => (!this.consuming ? resolve() : scheduleWait()), 1000)
      }

      if (!this.consuming) {
        return resolve()
      }

      scheduleWait()
    })
  }

  async processEachMessage(batch) {
    const { topic, partition } = batch

    for (let message of batch.messages) {
      if (!this.running) {
        break
      }

      try {
        await this.eachMessage({ topic, partition, message })
      } catch (e) {
        if (!isKafkaJSError(e)) {
          this.logger.error(`Error when calling eachMessage`, {
            topic,
            partition,
            offset: message.offset,
            stack: e.stack,
          })
        }

        // In case of errors, commit the previously consumed offsets
        await this.consumerGroup.commitOffsets()
        throw e
      }

      this.consumerGroup.resolveOffset({ topic, partition, offset: message.offset })
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch

    try {
      await this.eachBatch({
        batch,
        resolveOffset: offset => {
          this.consumerGroup.resolveOffset({ topic, partition, offset })
        },
        heartbeat: async () => {
          await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
        },
        isRunning: () => this.running,
      })
    } catch (e) {
      if (!isKafkaJSError(e)) {
        this.logger.error(`Error when calling eachBatch`, {
          topic,
          partition,
          offset: batch.firstOffset(),
          stack: e.stack,
        })
      }

      // eachBatch has a special resolveOffset which can be used
      // to keep track of the messages
      await this.consumerGroup.commitOffsets()
      throw e
    }

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      this.consumerGroup.resolveOffset({ topic, partition, offset: batch.lastOffset() })
    }
  }

  async fetch() {
    const batches = await this.consumerGroup.fetch()
    for (let batch of batches) {
      if (!this.running) {
        break
      }

      if (batch.isEmpty()) {
        this.consumerGroup.resetOffset(batch)
        continue
      }

      if (this.eachMessage) {
        await this.processEachMessage(batch)
      } else if (this.eachBatch) {
        await this.processEachBatch(batch)
      }
    }

    await this.consumerGroup.commitOffsets()
    await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
  }

  async scheduleFetch() {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
      return
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        this.consuming = true
        await this.fetch()
        this.consuming = false
        setImmediate(() => this.scheduleFetch())
      } catch (e) {
        if (isRebalancing(e)) {
          this.logger.error('The group is rebalancing, re-joining', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
            retryCount,
            retryTime,
          })

          await this.join()
          this.scheduleFetch()
          return
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          this.logger.error('The coordinator is not aware of this member, re-joining the group', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
            retryCount,
            retryTime,
          })

          this.consumerGroup.memberId = null
          await this.join()
          this.scheduleFetch()
          return
        }

        if (e.name === 'KafkaJSOffsetOutOfRange') {
          this.scheduleFetch()
          return
        }

        this.logger.debug('Error while fetching data, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: e.message,
          stack: e.stack,
          retryCount,
          retryTime,
        })

        throw e
      } finally {
        this.consuming = false
      }
    }).catch(this.onCrash)
  }
}
