const { KafkaJSNonRetriableError } = require('../errors')

const REJECTED_ERROR = new KafkaJSNonRetriableError(
  'Queued function aborted due to earlier promise rejection'
)
function NOOP() {}

const concurrency = ({ limit, onChange = NOOP } = {}) => {
  if (typeof limit !== 'number' || limit < 1) {
    throw new KafkaJSNonRetriableError(`"limit" cannot be less than 1`)
  }

  let waiting = []
  let semaphore = 0

  const clear = () => {
    waiting.forEach(fn => {
      fn((fn, resolve, reject) => reject(REJECTED_ERROR))
    })
    waiting = []
    semaphore = 0
  }

  const next = () => {
    semaphore--
    onChange(semaphore)

    if (waiting.length > 0) {
      const fn = waiting.pop()
      fn()
    }
  }

  const invoke = (fn, resolve, reject) => {
    semaphore++
    onChange(semaphore)

    fn()
      .then(result => {
        resolve(result)
        next()
      })
      .catch(error => {
        reject(error)
        clear()
      })
  }

  const push = (fn, resolve, reject) => {
    if (semaphore < limit) {
      invoke(fn, resolve, reject)
    } else {
      waiting.push((overrideInvokation = invoke) => overrideInvokation(fn, resolve, reject))
    }
  }

  return fn => new Promise((resolve, reject) => push(fn, resolve, reject))
}

module.exports = concurrency
