const { KafkaJSLockTimeout } = require('../errors')

const PRIVATE = {
  LOCKED: Symbol('private:Lock:locked'),
  TIMEOUT: Symbol('private:Lock:timeout'),
  WAITING: Symbol('private:Lock:waiting'),
  TIMEOUT_ERROR_MESSAGE: Symbol('private:Lock:timeoutErrorMessage'),
}

const TIMEOUT_MESSAGE = 'Timeout while acquiring lock'

module.exports = class Lock {
  constructor({ timeout = 1000, description = null } = {}) {
    this[PRIVATE.LOCKED] = false
    this[PRIVATE.TIMEOUT] = timeout
    this[PRIVATE.WAITING] = new Set()
    this[PRIVATE.TIMEOUT_ERROR_MESSAGE] = description
      ? `${TIMEOUT_MESSAGE}: "${description}"`
      : TIMEOUT_MESSAGE
  }

  async acquire() {
    return new Promise((resolve, reject) => {
      if (!this[PRIVATE.LOCKED]) {
        this[PRIVATE.LOCKED] = true
        return resolve()
      }

      let timeoutId
      const tryToAcquire = async () => {
        if (!this[PRIVATE.LOCKED]) {
          this[PRIVATE.LOCKED] = true
          clearTimeout(timeoutId)
          this[PRIVATE.WAITING].delete(tryToAcquire)
          return resolve()
        }
      }

      this[PRIVATE.WAITING].add(tryToAcquire)
      timeoutId = setTimeout(
        () => reject(new KafkaJSLockTimeout(this[PRIVATE.TIMEOUT_ERROR_MESSAGE])),
        this[PRIVATE.TIMEOUT]
      )
    })
  }

  async release() {
    this[PRIVATE.LOCKED] = false
    const waitingForLock = Array.from(this[PRIVATE.WAITING])
    return Promise.all(waitingForLock.map(acquireLock => acquireLock()))
  }
}
