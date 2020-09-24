const Encoder = require('../../encoder')
const { KafkaJSNonRetriableError } = require('../../../errors')
const {
  createLogger,
  LEVELS: { INFO },
} = require('../../../loggers')
const LoggerConsole = require('../../../loggers/console')

const { workerThreads, failIfWorkerThreadsNotAvailable } = require('./workerThreads')
const { lookupCodecByRecordBatchAttributes, lookupCodec } = require('./index')
const { workerTaskType, workerResultType } = require('./workerPoolTaskType')

failIfWorkerThreadsNotAvailable()
const { isMainThread, workerData, threadId, parentPort } = workerThreads

if (isMainThread) {
  throw new KafkaJSNonRetriableError(
    'The compression worker cannot be initialized on the main thread'
  )
}

const { logLevel, setupScriptPath } = workerData
const setupWorker = require(setupScriptPath)
const { logCreator } = setupWorker()

const rootLogger = createLogger({
  level: logLevel || INFO,
  logCreator: logCreator || LoggerConsole,
})

const threadLogger = rootLogger.namespace(`CompressionWorkerThread#${threadId}`)

const decompressTask = async ({ attributes, compressedRecordsBuffer }) => {
  const codec = lookupCodecByRecordBatchAttributes(attributes)
  const decompressedRecordBuffer = await codec.decompress(compressedRecordsBuffer)
  const sharedBuffer = new SharedArrayBuffer(decompressedRecordBuffer.length + 1)
  const sharedArray = new Uint8Array(sharedBuffer)

  sharedArray[0] = workerResultType.SUCCESS
  sharedArray.set(decompressedRecordBuffer, 1)

  parentPort.postMessage(sharedArray)
}

const compressTask = async ({ compressionType, records }) => {
  const codec = lookupCodec(compressionType)
  const compressedRecords = await codec.compress(new Encoder().writeBuffer(Buffer.from(records)))
  const sharedBuffer = new SharedArrayBuffer(compressedRecords.length + 1)
  const sharedArray = new Uint8Array(sharedBuffer)

  sharedArray[0] = workerResultType.SUCCESS
  sharedArray.set(compressedRecords, 1)

  parentPort.postMessage(sharedArray)
}

const handleError = e => {
  const errorMessage = Buffer.from(e.message)
  const sharedBuffer = new SharedArrayBuffer(errorMessage.length + 1)
  const sharedArray = new Uint8Array(sharedBuffer)

  sharedArray[0] = workerResultType.FAILURE
  sharedArray.set(errorMessage, 1)

  parentPort.postMessage(sharedArray)
}

parentPort.on('message', sharedBuffer => {
  const sharedArray = new Uint8Array(sharedBuffer)
  const taskType = sharedArray[0]

  switch (taskType) {
    case workerTaskType.COMPRESS: {
      const compressionType = sharedArray[1]
      const records = Buffer.from(sharedArray.slice(2, sharedArray.length))

      compressTask({ compressionType, records }).catch(e => {
        threadLogger.error('Failed to compress data', {
          error: e.message,
          stack: e.stack,
        })

        handleError(e)
      })
      break
    }

    case workerTaskType.DECOMPRESS: {
      const attributes = sharedArray[1]
      const compressedRecordsBuffer = Buffer.from(sharedArray.slice(2, sharedArray.length))

      decompressTask({ attributes, compressedRecordsBuffer }).catch(e => {
        threadLogger.error('Failed to decompress data', {
          error: e.message,
          stack: e.stack,
        })

        handleError(e)
      })
      break
    }
  }
})

threadLogger.debug('Worker initialized', { threadId })
