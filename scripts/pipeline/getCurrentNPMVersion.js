const https = require('https')

const getCurrentVersion = async () =>
  new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: 'https:',
        host: 'registry.npmjs.org',
        path: `/kafkajs`,
        headers: {
          'User-Agent': 'KafkaJS Azure Pipeline',
        },
      },
      res => {
        let rawData = ''

        res.setEncoding('utf8')
        res.on('data', chunk => (rawData += chunk))
        res.on('end', () => {
          try {
            if (res.statusCode !== 200) {
              return reject(
                new Error(`Error getting current NPM version: ${res.statusCode} - ${rawData}`)
              )
            }

            const data = JSON.parse(rawData)
            resolve(data['dist-tags'])
          } catch (e) {
            reject(e)
          }
        })
      }
    )

    request.on('error', reject)
    request.end()
  })

module.exports = getCurrentVersion
