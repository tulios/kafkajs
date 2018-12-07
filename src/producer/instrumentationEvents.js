const swapObject = require('../utils/swapObject')
const networkEvents = require('../network/InstrumentationEvents')
const InstrumentationEventType = require('../instrumentation/eventType')
const producerType = InstrumentationEventType('producer')

const events = {
  CONNECT: producerType('connect'),
  DISCONNECT: producerType('disconnect'),
  REQUEST: producerType(networkEvents.NETWORK_REQUEST),
  REQUEST_TIMEOUT: producerType(networkEvents.NETWORK_REQUEST_TIMEOUT),
}

const wrappedEvents = {
  [events.REQUEST]: networkEvents.NETWORK_REQUEST,
  [events.REQUEST_TIMEOUT]: networkEvents.NETWORK_REQUEST_TIMEOUT,
}

const reversedWrappedEvents = swapObject(wrappedEvents)
const unwrap = eventName => wrappedEvents[eventName] || eventName
const wrap = eventName => reversedWrappedEvents[eventName] || eventName

module.exports = {
  events,
  wrap,
  unwrap,
}
