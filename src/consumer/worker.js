/**
 * @typedef {(batch: T, metadata: { workerId: number }) => Promise<void>} Handler
 * @template T
 *
 * @typedef {ReturnType<typeof createWorker>} Worker
 */

/**
 * @param {{ handler: Handler<T>, workerId: number }} options
 * @template T
 */
const createWorker = ({ handler, workerId }) => {
  const getWorkerId = () => workerId

  /**
   * Takes messages from next() until it returns undefined.
   *
   * @param {{ next: () => T | undefined }} param0
   * @returns {Promise<void>}
   */
  const run = async ({ next }) => {
    const batch = next()
    if (!batch) {
      return
    }

    await handler(batch, { workerId })
    return run({ next })
  }

  return { getWorkerId, run }
}

module.exports = createWorker
