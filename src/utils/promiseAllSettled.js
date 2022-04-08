/**
 *
 * @param { Promise<T>[] } promises
 * @returns {Promise<[{ status: "fulfilled", value: T} | { status: "rejected", reason: Error}]> }
 * @template T
 */

function allSettled(promises) {
  const wrappedPromises = promises.map(p =>
    Promise.resolve(p).then(
      val => ({ status: 'fulfilled', value: val }),
      err => ({ status: 'rejected', reason: err })
    )
  )
  return Promise.all(wrappedPromises)
}

module.exports = allSettled
