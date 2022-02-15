const allSettled = require('../utils/promiseAllSettled')

/**
 * @typedef {ReturnType<typeof createWorkerQueue>} WorkerQueue
 */

/**
 *
 * @param {object} options
 * @param {import('./worker').Worker<T>[]} options.workers
 * @template T
 */
const createWorkerQueue = ({ workers }) => {
  /** @type {T[]} */
  const queue = []

  /**
   * @param {...T} batches
   * @returns {Promise<void>}
   */
  const push = async (...batches) => {
    queue.push(...batches)

    const results = await allSettled(
      workers.map(worker =>
        worker.run({
          next: () => queue.shift(),
        })
      )
    )

    const rejected = results.find(result => result.status === 'rejected')
    if (rejected) {
      // @ts-ignore
      throw rejected.reason
    }
  }

  return { push }
}

module.exports = createWorkerQueue
