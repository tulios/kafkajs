const { KafkaJSNotImplemented } = require('../../../errors')

let workerThreads = null
try {
  workerThreads = require('worker_threads') // eslint-disable-line node/no-unsupported-features/node-builtins
} catch (e) {}

const isWorkerThreadsAvailable = () => !!workerThreads
const failIfWorkerThreadsNotAvailable = () => {
  if (!isWorkerThreadsAvailable()) {
    throw new KafkaJSNotImplemented(
      `The module "worker_threads" is not supported until Node.js 12.11.0, current version ${process.env}`
    )
  }
}

module.exports = {
  workerThreads,
  isWorkerThreadsAvailable,
  failIfWorkerThreadsNotAvailable,
}
