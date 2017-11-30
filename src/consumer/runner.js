const createRetry = require('../retry')

module.exports = class Runner {
  constructor({
    consumerGroup,
    logger,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    onCrash,
    retry,
  }) {
    this.consumerGroup = consumerGroup
    this.logger = logger.namespace('Runner')
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    this.retrier = createRetry(Object.assign({}, retry))
    this.onCrash = onCrash

    this.running = false
    this.consuming = false
  }

  async join() {
    await this.consumerGroup.join()
    await this.consumerGroup.sync()
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
      await this.waitForConsumer()
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
      })
    } catch (e) {
      // eachBatch has a special resolveOffset which can be used
      // to keep track of the messages
      await this.consumerGroup.commitOffsets()
      throw e
    }

    this.consumerGroup.resolveOffset({ topic, partition, offset: batch.lastOffset() })
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
        this.scheduleFetch()
      } catch (e) {
        if (!this.consumerGroup.cluster.isConnected()) {
          this.logger.error(`Cluster has disconnected, reconnecting: ${e.message}`, {
            retryCount,
            retryTime,
          })
          await this.consumerGroup.cluster.connect()
          await this.consumerGroup.cluster.refreshMetadata()
          this.scheduleFetch()
          return
        }

        if (e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP') {
          this.logger.error('The group is rebalancing, re-joining', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
          })

          await this.join()
          this.scheduleFetch()
          return
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          this.consumerGroup.memberId = null
          await this.join()
          this.scheduleFetch()
          return
        }

        this.logger.debug('Error while fetching data, trying again...', {
          error: e.message,
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
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
