/**
 * @typedef {(batch: T, metadata: { workerId: number }) => Promise<void>} Handler
 * @template T
 *
 * @typedef {ReturnType<typeof createWorker>} Worker
 */

const sharedPromiseTo = require('../utils/sharedPromiseTo')

/**
 * @param {{ handler: Handler<T>, workerId: number, partitionAssignments: Map, logger: import('../../types').Logger }} options
 * @template T
 */
const createWorker = ({ handler, workerId, partitionAssignments, logger: rootLogger }) => {
  const logger = rootLogger.namespace(`Worker ${workerId}`)
  /**
   * Takes batches from next() until it returns undefined.
   *
   * @param {{ next: () => { batch: T, resolve: () => void, reject: (e: Error) => void } | undefined }} param0
   * @returns {Promise<void>}
   */
  const run = sharedPromiseTo(async ({ next }) => {
    while (true) {
      const item = next()
      if (!item) break

      const { batch, resolve, reject } = item
      const { topic, partition } = batch
      const key = `${topic}|${partition}`
      if (partitionAssignments.has(key)) {
        logger.info('Skipping batch due to partition already being assigned to another worker', {
          assignedWorker: partitionAssignments[partition],
          topic,
          partition,
        })
        continue
      }
      partitionAssignments.set(key, workerId)
      try {
        await handler(batch, { workerId })
        partitionAssignments.delete(key)
        resolve()
      } catch (error) {
        partitionAssignments.delete(key)
        reject(error)
      }
    }
  })

  return { run }
}

module.exports = createWorker
