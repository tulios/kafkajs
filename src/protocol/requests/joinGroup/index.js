const NETWORK_DELAY = 5000
const versions = {
  0: ({ groupId, sessionTimeout, memberId, protocolType, groupProtocols }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')

    /**
     * @see https://github.com/apache/kafka/pull/5203
     * The JOIN_GROUP request may block up to sessionTimeout (or rebalanceTimeout in JoinGroupV1),
     * so we should override the requestTimeout to be a bit more than the sessionTimeout
     * NOTE: the sessionTimeout can be configured as Number.MAX_SAFE_INTEGER and overflow when
     * increased, so we have to check for potential overflows
     **/
    const requestTimeout = Number.isSafeInteger(sessionTimeout + NETWORK_DELAY)
      ? sessionTimeout + NETWORK_DELAY
      : sessionTimeout

    return {
      request: request({
        groupId,
        sessionTimeout,
        memberId,
        protocolType,
        groupProtocols,
      }),
      response,
      requestTimeout,
    }
  },
  1: ({ groupId, sessionTimeout, rebalanceTimeout, memberId, protocolType, groupProtocols }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')

    const timeout = rebalanceTimeout || sessionTimeout
    const requestTimeout = Number.isSafeInteger(timeout + NETWORK_DELAY)
      ? timeout + NETWORK_DELAY
      : timeout

    return {
      request: request({
        groupId,
        sessionTimeout,
        rebalanceTimeout,
        memberId,
        protocolType,
        groupProtocols,
      }),
      response,
      requestTimeout,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
