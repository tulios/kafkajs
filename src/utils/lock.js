const { KafkaJSLockTimeout } = require('../errors')

const PRIVATE = {
  LOCKED: Symbol('private:Lock:locked'),
  TIMEOUT: Symbol('private:Lock:timeout'),
  WAITING: Symbol('private:Lock:waiting'),
}

module.exports = class Lock {
  constructor({ timeout = 1000 } = {}) {
    this[PRIVATE.LOCKED] = false
    this[PRIVATE.TIMEOUT] = timeout
    this[PRIVATE.WAITING] = new Set()
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
        () => reject(new KafkaJSLockTimeout('Timeout while acquiring lock')),
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
