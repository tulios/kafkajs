const SnappyCodec = require('kafkajs-snappy')
const { CompressionTypes, CompressionCodecs } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

module.exports = () => {
  // configure Snappy codec
  CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec

  // Return extra configs for the worker (format/shape pending)
  return {
    logCreator: PrettyConsoleLogger,
  }
}
