const Encoder = require('../encoder')
const messageSet = require('./messageSet')

/**
* Produce Request (Version: 0) => acks timeout [topic_data]
*   acks => INT16
*   timeout => INT32
*   topic_data => topic [data]
*     topic => STRING
*     data => partition record_set
*       partition => INT32
*       record_set => RECORDS
*
* Fields:
* acks - The number of acknowledgments the producer requires the leader to have received
*        before considering a request complete. Allowed values: 0 for no acknowledgments,
*        1 for only the leader and -1 for the full ISR.
*
* timeout -	The time to await a response in ms.
*/

module.exports = ({ acks, timeout, topicData }) => {
  const encoder = new Encoder()
  encoder.writeInt16(acks)
  encoder.writeInt32(timeout)
  topicData.forEach(({ topic, data }) => {
    encoder.writeString(topic)
    data.forEach(({ partition, recordSet }) => {
      encoder.writeInt32(partition)
      encoder.writeEncoder(messageSet({ messages: recordSet }))
    })
  })

  return encoder
}
