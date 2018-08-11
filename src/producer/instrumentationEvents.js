const InstrumentationEventType = require('../instrumentation/eventType')
const producerType = InstrumentationEventType('producer')

module.exports = {
  CONNECT: producerType('connect'),
  DISCONNECT: producerType('disconnect'),
}
