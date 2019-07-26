#!/usr/bin/env node

const https = require('https')
const { coerce, prerelease, parse } = require('semver')

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

const sameStableVersion = (stable, beta) => coerce(stable).version === coerce(beta).version

getCurrentVersion().then(({ latest, beta }) => {
  console.log(`Current Latest: ${latest}, Beta: ${beta}`)
  const { major, minor } = parse(latest)
  const [tag, currentBeta] = prerelease(beta)
  const newStable = `${major}.${minor + 1}.0`
  const newBeta = sameStableVersion(newStable, beta) ? currentBeta + 1 : 0
  const newBetaVersion = `${newStable}-${tag}.${newBeta}`
  console.log(`New beta: ${newBetaVersion}`)
  console.log(`##vso[task.setvariable variable=PRE_RELEASE_VERSION;isOutput=true]${newBetaVersion}`)
})
