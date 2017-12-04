const EventEmitter = require('events')
const InstrumentationEvent = require('./event')
const { KafkaJSError } = require('../errors')

module.exports = class InstrumentationEventEmitter {
  constructor() {
    this.emitter = new EventEmitter()
  }

  emit(eventName, payload) {
    if (!eventName) {
      throw new KafkaJSError('Invalid event name', { retriable: false })
    }

    const event = new InstrumentationEvent(eventName, payload)
    this.emitter.emit(eventName, event)
  }

  addListener(eventName, listener) {
    this.emitter.addListener(eventName, listener)

    return () => this.emitter.removeListener(eventName, listener)
  }
}
