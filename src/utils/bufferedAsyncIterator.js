const EventEmitter = require('events')

const createPromiseNotifier = (emitter, results, handleError) => async (promise, i) => {
  try {
    const result = await promise
    if (emitter.listenerCount('data') > 0) {
      emitter.emit('data', { result, id: i })
    } else {
      results.push(result)
    }
  } catch (e) {
    try {
      await handleError(e)
      emitter.emit('error', e)
    } catch (newError) {
      emitter.emit('error', newError)
    }
  }
}

const createGetResolvedPromise = (emitter, results) => {
  let runningPromises = 0
  const fulfilledPromises = []

  return () => {
    runningPromises++
    return new Promise((resolve, reject) => {
      if (results.length > 0) {
        return resolve(results.shift())
      }

      const handler = ({ result, id }) => {
        /**
         * Since we have a single emitter for all running promises we have to skip
         * already delivered results as we will have one listener per promise
         * running, so once one promise resolves all of the listeners will receive
         * the same value
         */
        if (fulfilledPromises.includes(id)) {
          return
        }

        /**
         * When there is a single promise running, we can safely deliver the result
         * of the emitter since we won't have the risk of getting results from
         * other promises
         */
        if (runningPromises <= 1) {
          runningPromises--
          emitter.off('data', handler)

          return resolve(result)
        }

        /**
         * When multiple promises are running the emitter will receive data from all
         * running promises, thus the results can get mixed up.
         *
         * To avoid that and always unblock the first promises with the fastest results,
         * we need to keep track of the id, so we don't accidentally resolve the same
         * value multiple times.
         */
        runningPromises--
        emitter.off('data', handler)
        fulfilledPromises.push(id)

        resolve(result)
      }

      emitter.on('data', handler)
      emitter.on('error', reject)
    })
  }
}
const defaultErrorHandler = e => {
  throw e
}

function* BufferedAsyncIterator(promises, handleError = defaultErrorHandler) {
  const results = []
  const emitter = new EventEmitter()
  const wrap = createPromiseNotifier(emitter, results, handleError)
  const getResolvedPromise = createGetResolvedPromise(emitter, results)
  const wrappedPromises = promises.map(wrap)

  emitter.setMaxListeners(wrappedPromises.length)

  for (let i = 0; i < wrappedPromises.length; i++) {
    yield getResolvedPromise()
  }
}

module.exports = BufferedAsyncIterator
