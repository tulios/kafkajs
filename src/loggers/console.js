module.exports = level => (namespace, log) => {
  const label = namespace ? `[${namespace}] ` : ''
  const message = JSON.stringify(Object.assign(log, { message: `${label}${log.message}` }))

  switch (log.level) {
    case 'INFO':
      return console.info(message)
    case 'ERROR':
      return console.error(message)
    case 'WARN':
      return console.warn(message)
    case 'DEBUG':
      return console.log(message)
  }
}
