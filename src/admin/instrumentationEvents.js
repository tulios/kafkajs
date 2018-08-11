const InstrumentationEventType = require('../instrumentation/eventType')
const adminType = InstrumentationEventType('admin')

module.exports = {
  CONNECT: adminType('connect'),
  DISCONNECT: adminType('disconnect'),
}
