const EventEmitter = require('events')
const { KafkaJSLockTimeout } = require('../errors')

module.exports = class Lock {
  constructor({ timeout = 1000 } = {}) {
    this.locked = false
    this.timeout = timeout
    this.emitter = new EventEmitter()
  }

  async acquire() {
    return new Promise((resolve, reject) => {
      if (!this.locked) {
        this.locked = true
        return resolve()
      }

      let timeoutId
      const tryToAcquire = () => {
        if (!this.locked) {
          this.locked = true
          clearTimeout(timeoutId)
          this.emitter.removeListener('releaseLock', tryToAcquire)
          return resolve()
        }
      }

      this.emitter.on('releaseLock', tryToAcquire)
      timeoutId = setTimeout(
        () => reject(new KafkaJSLockTimeout('Timeout while acquiring lock')),
        this.timeout
      )
    })
  }

  async release() {
    this.locked = false
    setImmediate(() => this.emitter.emit('releaseLock'))
  }
}
