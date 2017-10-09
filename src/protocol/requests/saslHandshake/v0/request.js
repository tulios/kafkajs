const Encoder = require('../../../encoder')
const { SaslHandshake: apiKey } = require('../../apiKeys')
const MessageSet = require('../../../messageSet')

/**
 * SaslHandshake Request (Version: 0) => mechanism
 *    mechanism => STRING
 */

/**
 * @param {string} mechanism - SASL Mechanism chosen by the client
 */
module.exports = ({ mechanism }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'SaslHandshake',
  encode: () => new Encoder().writeString(mechanism),
})
