const { EventEmitter } = require('events')
const Long = require('../utils/long')
const createRetry = require('../retry')
const { isKafkaJSError } = require('../errors')

const {
  events: { START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')

const isSameOffset = (offsetA, offsetB) => Long.fromValue(offsetA).equals(Long.fromValue(offsetB))
const CONSUMING_START = 'consuming-start'
const CONSUMING_STOP = 'consuming-stop'

module.exports = class Runner extends EventEmitter {
  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("../instrumentation/emitter")} options.instrumentationEmitter
   * @param {boolean} [options.eachBatchAutoResolve=true]
   * @param {(payload: import("../../types").EachBatchPayload) => Promise<void>} [options.eachBatch]
   * @param {(payload: import("../../types").EachMessagePayload) => Promise<void>} [options.eachMessage]
   * @param {() => Promise<void>} options.heartbeat
   * @param {(callback: (batch: import('types').Batch) => Promise<void>) => Promise<void>} options.nextBatch
   * @param {(options: { topic: string, partition: number }) => boolean} options.hasSeekOffset
   * @param {(options: { topic: string, partition: number, offset: string }) => void} options.resolveOffset
   * @param {(offsets?: import('types').OffsetsByTopicPartition) => Promise<void>} options.commitOffsets
   * @param {() => Promise<void>} options.commitOffsetsIfNecessary
   * @param {() => import('types').OffsetsByTopicPartition} options.uncommittedOffsets
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {boolean} [options.autoCommit=true]
   */
  constructor({
    logger,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    eachBatch,
    eachMessage,
    heartbeat,
    nextBatch,
    hasSeekOffset,
    resolveOffset,
    commitOffsets,
    commitOffsetsIfNecessary,
    uncommittedOffsets,
    retry,
    autoCommit = true,
  }) {
    super()
    this.logger = logger
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.retrier = createRetry(Object.assign({}, retry))
    this.autoCommit = autoCommit
    this.heartbeat = heartbeat
    this.hasSeekOffset = hasSeekOffset
    this.resolveOffset = resolveOffset
    this.commitOffsets = commitOffsets
    this.commitOffsetsIfNecessary = commitOffsetsIfNecessary
    this.uncommittedOffsets = uncommittedOffsets
    this.nextBatch = nextBatch

    this.running = false
    this.consuming = false
  }

  get consuming() {
    return this._consuming
  }

  set consuming(value) {
    if (this._consuming !== value) {
      this._consuming = value
      this.emit(value ? CONSUMING_START : CONSUMING_STOP)
    }
  }

  /** @param {(e: Error) => void} onCrash */
  start(onCrash) {
    this.running = true
    this.scheduleConsume(onCrash)
  }

  async stop() {
    if (!this.running) return
    this.running = false

    this.logger.debug('stop consumer group')
    await this.waitForConsumer()
  }

  waitForConsumer() {
    return new Promise(resolve => {
      if (!this.consuming) {
        return resolve()
      }
      this.logger.debug('waiting for consumer to finish...')
      this.once(CONSUMING_STOP, () => resolve())
    })
  }

  async processEachMessage(batch) {
    const { topic, partition } = batch

    for (const message of batch.messages) {
      if (!this.running || this.hasSeekOffset({ topic, partition })) {
        break
      }

      try {
        await this.eachMessage({
          topic,
          partition,
          message,
          heartbeat: async () => {
            await this.heartbeat()
          },
        })
      } catch (e) {
        if (!isKafkaJSError(e)) {
          this.logger.error(`Error when calling eachMessage`, {
            topic,
            partition,
            offset: message.offset,
            stack: e.stack,
            error: e,
          })
        }

        // In case of errors, commit the previously consumed offsets unless autoCommit is disabled
        await this.autoCommitOffsets()
        throw e
      }

      this.resolveOffset({ topic, partition, offset: message.offset })
      await this.heartbeat()
      await this.autoCommitOffsetsIfNecessary()
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch
    const lastFilteredMessage = batch.messages[batch.messages.length - 1]

    try {
      await this.eachBatch({
        batch,
        resolveOffset: offset => {
          /**
           * The transactional producer generates a control record after committing the transaction.
           * The control record is the last record on the RecordBatch, and it is filtered before it
           * reaches the eachBatch callback. When disabling auto-resolve, the user-land code won't
           * be able to resolve the control record offset, since it never reaches the callback,
           * causing stuck consumers as the consumer will never move the offset marker.
           *
           * When the last offset of the batch is resolved, we should automatically resolve
           * the control record offset as this entry doesn't have any meaning to the user-land code,
           * and won't interfere with the stream processing.
           *
           * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
           */
          const offsetToResolve =
            lastFilteredMessage && isSameOffset(offset, lastFilteredMessage.offset)
              ? batch.lastOffset()
              : offset

          this.resolveOffset({ topic, partition, offset: offsetToResolve })
        },
        heartbeat: () => this.heartbeat(),
        /**
         * Commit offsets if provided. Otherwise commit most recent resolved offsets
         * if the autoCommit conditions are met.
         *
         * @param {import('../../types').OffsetsByTopicPartition} [offsets] Optional.
         */
        commitOffsetsIfNecessary: async offsets => {
          return offsets ? this.commitOffsets(offsets) : this.commitOffsetsIfNecessary()
        },
        uncommittedOffsets: () => this.uncommittedOffsets(),
        isRunning: () => this.running,
        isStale: () => this.hasSeekOffset({ topic, partition }),
      })
    } catch (e) {
      if (!isKafkaJSError(e)) {
        this.logger.error(`Error when calling eachBatch`, {
          topic,
          partition,
          offset: batch.firstOffset(),
          stack: e.stack,
          error: e,
        })
      }

      // eachBatch has a special resolveOffset which can be used
      // to keep track of the messages
      await this.autoCommitOffsets()
      throw e
    }

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      this.resolveOffset({ topic, partition, offset: batch.lastOffset() })
    }
  }

  /** @param {(e: Error) => void} onCrash */
  scheduleConsume(onCrash) {
    this.consuming = true

    const loop = async () => {
      if (!this.running) {
        this.consuming = false
        return
      }

      try {
        await this.consume()
      } catch (error) {
        this.consuming = false
        return onCrash(error)
      }

      return setImmediate(() => loop())
    }

    return setImmediate(() => loop())
  }

  /** @param {import('../../types').Batch} batch */
  async handleBatch(batch) {
    if (!this.running) return
    if (!batch) return

    const startBatchProcess = Date.now()
    const payload = {
      topic: batch.topic,
      partition: batch.partition,
      highWatermark: batch.highWatermark,
      offsetLag: batch.offsetLag(),
      /**
       * @since 2019-06-24 (>= 1.8.0)
       *
       * offsetLag returns the lag based on the latest offset in the batch, to
       * keep the event backward compatible we just introduced "offsetLagLow"
       * which calculates the lag based on the first offset in the batch
       */
      offsetLagLow: batch.offsetLagLow(),
      batchSize: batch.messages.length,
      firstOffset: batch.firstOffset(),
      lastOffset: batch.lastOffset(),
    }

    try {
      /**
       * If the batch contained only control records or only aborted messages then we still
       * need to resolve and auto-commit to ensure the consumer can move forward.
       *
       * We also need to emit batch instrumentation events to allow any listeners keeping
       * track of offsets to know about the latest point of consumption.
       *
       * Added in #1256
       *
       * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
       */
      if (batch.isEmptyDueToFiltering()) {
        this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

        this.resolveOffset({
          topic: batch.topic,
          partition: batch.partition,
          offset: batch.lastOffset(),
        })
        await this.autoCommitOffsetsIfNecessary()

        this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
          ...payload,
          duration: Date.now() - startBatchProcess,
        })
        return
      }

      if (batch.isEmpty()) {
        await this.heartbeat()
        return
      }

      this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

      if (this.eachMessage) {
        await this.processEachMessage(batch)
      } else if (this.eachBatch) {
        await this.processEachBatch(batch)
      }

      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      })

      await this.autoCommitOffsets()
      await this.heartbeat()
    } catch (error) {
      if (error.name === 'KafkaJSOffsetOutOfRange') {
        return this.handleBatch(batch)
      }
      throw error
    }
  }

  async consume() {
    await this.retrier(async (_, retryCount, retryTime) => {
      try {
        await this.nextBatch(batch => this.handleBatch(batch))
      } catch (e) {
        if (!this.running) {
          this.logger.debug('consumer not running, exiting', { error: e.message })
          return
        }

        if (e.retriable !== false) {
          this.logger.debug('Error while fetching data, trying again...', {
            error: e.message,
            stack: e.stack,
            retryCount,
            retryTime,
          })
        }
        throw e
      }
    })
  }

  autoCommitOffsets() {
    if (this.autoCommit) {
      return this.commitOffsets()
    }
  }

  autoCommitOffsetsIfNecessary() {
    if (this.autoCommit) {
      return this.commitOffsetsIfNecessary()
    }
  }
}
