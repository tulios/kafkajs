const { KafkaJSNonRetriableError } = require('../errors')

const REJECTED_ERROR = new KafkaJSNonRetriableError(
  'Queued function aborted due to earlier promise rejection'
)
function NOOP() {}

const concurrency = ({ limit, onChange = NOOP } = {}) => {
  if (isNaN(limit) || typeof limit !== 'number' || limit < 1) {
    throw new KafkaJSNonRetriableError(`"limit" cannot be less than 1`)
  }

  let waiting = []
  let semaphore = 0

  const clear = () => {
    for (const lazyAction of waiting) {
      lazyAction((_1, _2, reject) => reject(REJECTED_ERROR))
    }
    waiting = []
    semaphore = 0
  }

  const next = () => {
    semaphore--
    onChange(semaphore)

    if (waiting.length > 0) {
      const lazyAction = waiting.shift()
      lazyAction()
    }
  }

  const invoke = (action, resolve, reject) => {
    semaphore++
    onChange(semaphore)

    action()
      .then(result => {
        resolve(result)
        next()
      })
      .catch(error => {
        reject(error)
        clear()
      })
  }

  const push = (action, resolve, reject) => {
    if (semaphore < limit) {
      invoke(action, resolve, reject)
    } else {
      waiting.push(override => {
        const execute = override || invoke
        execute(action, resolve, reject)
      })
    }
  }

  return action => new Promise((resolve, reject) => push(action, resolve, reject))
}

module.exports = concurrency
