const Connection = require('./connection')
const API = require('./api')

const connection = new Connection({ host: 'localhost', port: 9092 })
connection
  .connect()
  .then(async () => {
    const api = new API(connection)
    await api.apiVersions()
    await api.metadata(['topic1'])
  })
  .catch(error => {
    console.error(error)
    connection.disconnect()
  })

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['exit', 'SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      connection.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      connection.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
