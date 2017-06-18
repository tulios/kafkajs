const net = require('net')
const createRequest = require('./protocol/request')
const Encoder = require('./protocol/encoder')
const Decoder = require('./protocol/decoder')

module.exports = class Connection {
  constructor({ host, port }) {
    this.host = host
    this.port = port

    this.connected = false
    this.correlationId = 0
    this.queue = {}
  }

  nextCorrelationId() {
    if (this.correlationId === Number.MAX_VALUE) {
      this.correlationId = 0
    }

    return this.correlationId++
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.connect({ host: this.host, port: this.port }, () => {
        this.connected = true
        resolve()
      })

      this.socket.on('data', data => {
        const decoder = new Decoder(data)
        const size = decoder.readInt32()
        const correlationId = decoder.readInt32()
        const response = decoder.readAll()

        const entry = this.queue[correlationId]

        if (!entry) {
          console.error(`Response without match for correlation id ${correlationId}`)
          return
        }

        console.log(
          `Response ${entry.apiName}(key: ${entry.apiKey}, version: ${entry.apiVersion}) correlationId: ${correlationId} - ${size} bytes`
        )
        entry.handler(response)
      })
      this.socket.on('end', () => {
        console.log('Kafka server has closed connection')
        // this.disconnect()
      })

      this.socket.on('error', error => {
        console.error(`Connection error`, error)
        reject(error)
        this.disconnect()
      })
    })
  }

  disconnect() {
    console.log('disconnecting...')
    if (!this.connected) {
      console.log('already disconnected')
      return
    }

    this.socket.end()
    this.connected = false
    console.log('disconnected')
  }

  send({ request, response }) {
    const correlationId = this.nextCorrelationId()

    console.log(
      `Request ${request.apiName}(key: ${request.apiKey}, version: ${request.apiVersion}) correlationId: ${correlationId}`
    )

    const encoder = new Encoder()
    const requestPayload = createRequest({
      request,
      correlationId,
      clientId: 'abc',
    })

    return new Promise(resolve => {
      this.queue[correlationId] = {
        apiKey: request.apiKey,
        apiName: request.apiName,
        apiVersion: request.apiVersion,
        handler: data => resolve(response(data)),
      }
      this.socket.write(requestPayload.buffer, 'binary')
    })
  }
}
