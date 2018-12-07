const InstrumentationEventType = require('../instrumentation/eventType')
const eventType = InstrumentationEventType('network')

module.exports = {
  NETWORK_REQUEST: eventType('request'),
  NETWORK_REQUEST_TIMEOUT: eventType('request_timeout'),
}
