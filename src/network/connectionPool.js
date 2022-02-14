const apiKeys = require('../protocol/requests/apiKeys')
const Connection = require('./connection')

const { assign } = Object

module.exports = class ConnectionPool {
  constructor(options) {
    assign(this, options)

    this.pool = new Array(2).fill().map(() => new Connection(options))
  }

  get connected() {
    return this.pool.every(c => c.connected)
  }

  connectionByProtocolRequest(protocolRequest) {
    const {
      request: { apiKey },
    } = protocolRequest
    const index = { [apiKeys.Fetch]: 1 }[apiKey] || 0
    return this.pool[index]
  }

  async connect() {
    await Promise.all(this.pool.map(c => c.connect()))
  }

  async disconnect() {
    await Promise.all(this.pool.map(c => c.disconnect()))
  }

  send(protocolRequest) {
    const connection = this.connectionByProtocolRequest(protocolRequest)
    return connection.send(protocolRequest)
  }

  getConnection() {
    return this.pool[0]
  }

  async all(callback) {
    await Promise.all(this.pool.map(callback))
  }
}
