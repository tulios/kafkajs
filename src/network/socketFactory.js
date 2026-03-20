const KEEP_ALIVE_DELAY_MS = 60000

/**
 * @returns {import("../../types").ISocketFactory}
 */
module.exports = () => {
  const net = require('net')
  const tls = require('tls')

  return ({ host, port, ssl, onConnect }) => {
    const socket = ssl
      ? tls.connect(
          Object.assign({ host, port }, !net.isIP(host) ? { servername: host } : {}, ssl),
          onConnect
        )
      : net.connect({ host, port }, onConnect)

    socket.setKeepAlive(true, KEEP_ALIVE_DELAY_MS)

    return socket
  }
}
