const net = require('net')
const tls = require('tls')

const KEEP_ALIVE_DELAY = 60000 // in ms

module.exports = ({ host, port, ssl, onConnect, onData, onEnd, onError, onTimeout }) => {
  const socket = ssl
    ? tls.connect(
        Object.assign({ host, port }, ssl),
        onConnect
      )
    : net.connect(
        { host, port },
        onConnect
      )

  socket.setKeepAlive(true, KEEP_ALIVE_DELAY)
  socket.on('data', onData)
  socket.on('end', onEnd)
  socket.on('error', onError)
  socket.on('timeout', onTimeout)

  return socket
}
