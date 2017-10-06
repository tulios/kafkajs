const RETRY_DEFAULT = {
  maxRetryTime: 30 * 1000,
  initialRetryTime: 300,
  factor: 0.2, // randomization factor
  multiplier: 2, // exponential factor
  retries: Infinity, // max retries
  logger: null,
}

const random = (min, max) => {
  return Math.random() * (max - min) + min
}

const randomFromRetryTime = (factor, retryTime) => {
  const delta = factor * retryTime
  return Math.ceil(random(retryTime - delta, retryTime + delta))
}

const createRetriable = (configs, resolve, reject, fn) => {
  let aborted = false
  const { factor, multiplier, maxRetryTime, retries } = configs

  const bail = error => {
    aborted = true
    reject(error || new Error('Aborted'))
  }

  const calculateExponentialRetryTime = retryTime => {
    return Math.min(randomFromRetryTime(factor, retryTime) * multiplier, maxRetryTime)
  }

  const retry = (retryTime, retryCount = 0) => {
    if (aborted) return

    const nextRetryTime = calculateExponentialRetryTime(retryTime)
    const shouldRetry = retryCount < retries

    const scheduleRetry = () => {
      setTimeout(() => retry(nextRetryTime, retryCount + 1), retryTime)
    }

    fn(bail, retryCount, retryTime)
      .then(resolve)
      .catch(e => {
        if (configs.logger) {
          configs.logger.error(e.message, { stack: e.stack })
        }
        shouldRetry ? scheduleRetry() : reject(e)
      })
  }

  return retry
}

module.exports = (opts = {}) => fn => {
  return new Promise((resolve, reject) => {
    const configs = Object.assign({}, RETRY_DEFAULT, opts)
    const start = createRetriable(configs, resolve, reject, fn)
    start(randomFromRetryTime(configs.factor, configs.initialRetryTime))
  })
}
