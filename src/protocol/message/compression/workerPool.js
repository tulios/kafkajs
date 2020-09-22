const path = require('path')
const { EventEmitter } = require('events')
const { KafkaJSNonRetriableError } = require('../../../errors')

const {
  workerThreads,
  isWorkerThreadsAvailable,
  failIfWorkerThreadsNotAvailable,
} = require('./workerThreads')

const { workerTaskType, workerResultType } = require('./workerPoolTaskType')

const WORKER_PATH = path.join(__dirname, './worker.js')
const PRIVATE = {
  WORKER_ID: Symbol('private:CompressionWorkerPool:workerId'),
  WORKER_STATUS: Symbol('private:CompressionWorkerPool:workerStatus'),
  WORKERS: Symbol('private:CompressionWorkerPool:workers'),
  CREATE_WORKER: Symbol('private:CompressionWorkerPool:createWorker'),
  POST_MESSAGE: Symbol('private:CompressionWorkerPool:postMessage'),
}

const WORKER_STATUS = {
  AVAILABLE: 'available',
  BUSY: 'busy',
}

const INTERNAL_EVENTS = {
  WORKER_AVAILABLE: 'worker-available',
}

let globalWorkersId = 0
const nextWorkerId = () => {
  return ++globalWorkersId
}

class CompressionWorkerPool extends EventEmitter {
  /**
   * @param {number} numberOfThreads. Default 1
   * @param {logLevels} logLevel
   * @param {string} logCreatorPath
   */
  constructor({ numberOfThreads, logLevel, logCreatorPath } = {}) {
    super()
    this.numberOfThreads = numberOfThreads || 1
    this[PRIVATE.WORKERS] = []

    for (let i = 0; i < numberOfThreads; i++) {
      this[PRIVATE.CREATE_WORKER]({ logLevel, logCreatorPath })
    }
  }

  close() {
    for (const worker of this[PRIVATE.WORKERS]) {
      worker.terminate()
    }
  }

  /**
   * @async
   * @public
   * @param {CompressionTypes} compression
   * @param {Buffer} records
   */
  compress(compressionType, records) {
    // TODO: throw if no workers
    const sharedBuffer = new SharedArrayBuffer(records.length + 2)
    const sharedArray = new Uint8Array(sharedBuffer)
    sharedArray[0] = workerTaskType.COMPRESS
    sharedArray[1] = compressionType
    sharedArray.set(records, 2)

    return new Promise((resolve, reject) => {
      this[PRIVATE.POST_MESSAGE](sharedBuffer, resolve, reject)
    })
  }

  /**
   * @async
   * @public
   * @param {Uint8} attributes
   * @param {Buffer} compressedRecordsBuffer
   */
  decompress(attributes, compressedRecordsBuffer) {
    // TODO: throw if no workers
    const sharedBuffer = new SharedArrayBuffer(compressedRecordsBuffer.length + 2)
    const sharedArray = new Uint8Array(sharedBuffer)
    sharedArray[0] = workerTaskType.DECOMPRESS
    sharedArray[1] = attributes
    sharedArray.set(compressedRecordsBuffer, 2)

    return new Promise((resolve, reject) => {
      this[PRIVATE.POST_MESSAGE](sharedBuffer, resolve, reject)
    })
  }

  [PRIVATE.POST_MESSAGE](payload, success, failure) {
    // TODO: if exiting, block new messages
    const findAvailableWorker = () =>
      this[PRIVATE.WORKERS].find(
        worker => worker[PRIVATE.WORKER_STATUS] === WORKER_STATUS.AVAILABLE
      )

    const sendPayload = worker => {
      worker[PRIVATE.WORKER_STATUS] = WORKER_STATUS.BUSY
      const onMessage = sharedBuffer => {
        try {
          const sharedArray = new Uint8Array(sharedBuffer)

          if (sharedArray[0] === workerResultType.FAILURE) {
            const errorMessage = Buffer.from(sharedArray.slice(1)).toString()
            return failure(new KafkaJSNonRetriableError(errorMessage))
          }

          success(Buffer.from(sharedArray.slice(1)))
        } finally {
          worker.removeListener('error', onError)
          worker[PRIVATE.WORKER_STATUS] = WORKER_STATUS.AVAILABLE
          this.emit(INTERNAL_EVENTS.WORKER_AVAILABLE)
        }
      }

      const onError = e => {
        failure(e)
        worker.removeListener('message', onMessage)
        worker[PRIVATE.WORKER_STATUS] = WORKER_STATUS.AVAILABLE
        this.emit(INTERNAL_EVENTS.WORKER_AVAILABLE)
      }

      worker.once('message', onMessage)
      worker.once('error', onError)

      worker.postMessage(payload)
    }

    const execute = () => {
      const worker = findAvailableWorker()
      worker ? sendPayload(worker) : this.once(INTERNAL_EVENTS.WORKER_AVAILABLE, execute)
    }

    execute()
  }

  [PRIVATE.CREATE_WORKER]({ logLevel, logCreatorPath }) {
    const worker = new workerThreads.Worker(WORKER_PATH, {
      workerData: { logLevel, logCreatorPath },
    })

    worker.unref()
    worker[PRIVATE.WORKER_ID] = nextWorkerId()
    worker[PRIVATE.WORKER_STATUS] = WORKER_STATUS.AVAILABLE
    worker.on('exit', code => {
      const index = this[PRIVATE.WORKERS].indexOf(worker)
      this[PRIVATE.WORKERS].splice(index, 1)
    })

    this[PRIVATE.WORKERS].push(worker)
  }
}

let compressionWorkerPool

/**
 * @param {number} numberOfThreads
 * @param {logLevels} logLevel
 * @param {string} logCreatorPath
 */
const createCompressionWorkerPool = args => {
  failIfWorkerThreadsNotAvailable()

  if (!compressionWorkerPool) {
    compressionWorkerPool = new CompressionWorkerPool(args)
    return true
  }

  return false
}

const getCompressionWorkerPool = () => compressionWorkerPool
const isCompressionWorkerPoolAvailable = () =>
  isWorkerThreadsAvailable() && getCompressionWorkerPool() != null

module.exports = {
  createCompressionWorkerPool,
  getCompressionWorkerPool,
  isCompressionWorkerPoolAvailable,
  isWorkerThreadsAvailable,
}
