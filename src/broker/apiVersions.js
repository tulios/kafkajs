const { requests } = require('../protocol/requests')

module.exports = async connection => {
  const apiVersions = requests.ApiVersions.protocol({ version: 0 })
  const response = await connection.send(apiVersions())
  return response.apiVersions.reduce(
    (obj, version) =>
      Object.assign(obj, {
        [version.apiKey]: {
          minVersion: version.minVersion,
          maxVersion: version.maxVersion,
        },
      }),
    {}
  )
}
