const { SUCCESS_CODE, KafkaProtocolError } = require('./protocol/error')
const { apiName } = require('./protocol/apiKeys')

const APIVersionsProtocol = require('./protocol/requests/apiVersions')
const MetadataProtocol = require('./protocol/requests/metadata')

const success = code => code === SUCCESS_CODE

module.exports = class API {
  constructor(connection) {
    this.connection = connection
  }

  async apiVersions() {
    const apiVersions = APIVersionsProtocol({ version: 0 })
    const response = await this.connection.send(apiVersions())
    if (!success(response.errorCode)) {
      throw new KafkaProtocolError(response.errorCode)
    }

    const versions = response.apiVersions.map(version =>
      Object.assign(version, {
        apiName: apiName(version.apiKey),
      })
    )

    return versions
  }

  async metadata(topics) {
    const metadata = MetadataProtocol({ version: 2 })
    const response = await this.connection.send(metadata(topics))
    console.log(JSON.stringify(response))
  }
}
