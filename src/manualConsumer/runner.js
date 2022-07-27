const { EventEmitter } = require('events')
const Long = require('../utils/long')
const createRetry = require('../retry')
const { isKafkaJSError } = require('../errors')

const {
  events: { FETCH, FETCH_START, START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('../consumer/instrumentationEvents')
const createFetchManager = require('../consumer/fetchManager')

const isSameOffset = (offsetA, offsetB) => Long.fromValue(offsetA).equals(Long.fromValue(offsetB))
const CONSUMING_START = 'consuming-start'
const CONSUMING_STOP = 'consuming-stop'

module.exports = class Runner extends EventEmitter {
  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("./manualConsumer")} options.manualConsumer
   * @param {import("../instrumentation/emitter")} options.instrumentationEmitter
   * @param {boolean} [options.eachBatchAutoResolve=true]
   * @param {number} options.concurrency
   * @param {(payload: import("../../types").EachBatchPayload) => Promise<void>} [options.eachBatch]
   * @param {(payload: import("../../types").EachMessagePayload) => Promise<void>} [options.eachMessage]
   * @param {(reason: Error) => void} options.onCrash
   * @param {import("../../types").RetryOptions} [options.retry]
   */
  constructor({
    logger,
    manualConsumer,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    concurrency,
    eachBatch,
    eachMessage,
    onCrash,
    retry,
  }) {
    super()
    this.logger = logger.namespace('Runner')
    this.manualConsumer = manualConsumer
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.retrier = createRetry(Object.assign({}, retry))
    this.onCrash = onCrash
    this.fetchManager = createFetchManager({
      logger: this.logger,
      getNodeIds: () => this.manualConsumer.getNodeIds(),
      fetch: nodeId => this.fetch(nodeId),
      handler: batch => this.handleBatch(batch),
      concurrency,
    })

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

  async start() {
    if (this.running) {
      return
    }

    try {
      await this.manualConsumer.connect()
      await this.manualConsumer.assignPartitions()
    } catch (e) {
      return this.onCrash(e)
    }

    this.running = true
    this.scheduleFetchManager()
  }

  scheduleFetchManager() {
    if (!this.running) {
      this.consuming = false

      this.logger.info('consumer not running, exiting')

      return
    }

    this.consuming = true

    this.retrier(async (bail, retryCount, retryTime) => {
      if (!this.running) {
        return
      }

      try {
        await this.fetchManager.start()
      } catch (e) {
        if (e.name === 'KafkaJSNotImplemented') {
          return bail(e)
        }

        if (e.name === 'KafkaJSConnectionError') {
          return bail(e)
        }

        this.logger.debug('Error while scheduling fetch manager, trying again...', {
          error: e.message,
          stack: e.stack,
          retryCount,
          retryTime,
        })

        throw e
      }
    })
      .then(() => {
        this.scheduleFetchManager()
      })
      .catch(e => {
        this.onCrash(e)
        this.consuming = false
        this.running = false
      })
  }

  async stop() {
    if (!this.running) {
      return
    }

    this.logger.debug('stop consumer')

    this.running = false

    try {
      await this.fetchManager.stop()
      await this.waitForConsumer()
    } catch (e) {}
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

    const pause = () => {
      this.manualConsumer.pause([{ topic, partitions: [partition] }])
      return () => this.manualConsumer.resume([{ topic, partitions: [partition] }])
    }
    for (const message of batch.messages) {
      if (!this.running || this.manualConsumer.hasSeekOffset({ topic, partition })) {
        break
      }

      try {
        await this.eachMessage({
          topic,
          partition,
          message,
          pause,
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
        throw e
      }

      this.manualConsumer.resolveOffset({ topic, partition, offset: message.offset })

      if (this.manualConsumer.isPaused(topic, partition)) {
        break
      }
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch
    const lastFilteredMessage = batch.messages[batch.messages.length - 1]

    const pause = () => {
      this.manualConsumer.pause([{ topic, partitions: [partition] }])
      return () => this.manualConsumer.resume([{ topic, partitions: [partition] }])
    }

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

          this.manualConsumer.resolveOffset({ topic, partition, offset: offsetToResolve })
        },
        pause,
        isRunning: () => this.running,
        isStale: () => this.manualConsumer.hasSeekOffset({ topic, partition }),
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
      throw e
    }

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      this.manualConsumer.resolveOffset({ topic, partition, offset: batch.lastOffset() })
    }
  }

  async fetch(nodeId) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting')

      return []
    }

    const startFetch = Date.now()

    this.instrumentationEmitter.emit(FETCH_START, { nodeId })

    const batches = await this.manualConsumer.fetch(nodeId)

    this.instrumentationEmitter.emit(FETCH, {
      /**
       * PR #570 removed support for the number of batches in this instrumentation event;
       * The new implementation uses an async generation to deliver the batches, which makes
       * this number impossible to get. The number is set to 0 to keep the event backward
       * compatible until we bump KafkaJS to version 2, following the end of node 8 LTS.
       *
       * @since 2019-11-29
       */
      numberOfBatches: 0,
      duration: Date.now() - startFetch,
      nodeId,
    })

    return batches
  }

  async handleBatch(batch) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting')

      return
    }

    /** @param {import('./batch')} batch */
    const onBatch = async batch => {
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

      /**
       * If the batch contained only control records or only aborted messages then we still
       * need to resolve to ensure the consumer can move forward.
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
        this.manualConsumer.resolveOffset({
          topic: batch.topic,
          partition: batch.partition,
          offset: batch.lastOffset(),
        })
        this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
          ...payload,
          duration: Date.now() - startBatchProcess,
        })
        return
      }

      if (batch.isEmpty()) {
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
    }

    await onBatch(batch)
  }
}
