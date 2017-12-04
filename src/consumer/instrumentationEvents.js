const InstrumentationEventType = require('../instrumentation/eventType')
const consumerType = InstrumentationEventType('consumer')

module.exports = {
  HEARTBEAT: consumerType('heartbeat'),
  COMMIT_OFFSETS: consumerType('commit_offsets'),
}
