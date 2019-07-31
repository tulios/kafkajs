#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const https = require('https')

let { TAG, TOKEN } = process.env

if (!TAG) {
  throw new Error('Missing TAG env variable')
}

if (!TOKEN) {
  throw new Error('Missing TOKEN env variable')
}

if (TAG.startsWith('refs/tags/')) {
  TAG = TAG.replace('refs/tags/', '')
}

console.log(`-> Updating release ${TAG}`)

const changelog = fs.readFileSync(path.join(__dirname, '../../CHANGELOG.md'), 'utf-8')
const lines = changelog.split('\n')

const RELEASE_HEADER = /^\s*##\s*\[\d+\.\d+\.\d+(-beta\.\d)?\]\s*-\s*\d{4}-\d{2}-\d{2}\s*$/

while (!RELEASE_HEADER.test(lines[0])) {
  lines.shift()
}

const releases = {}
let buffer = []

const getVersionNumber = () => buffer[0].match(/\[(.*)\]/)[1]

for (const line of lines) {
  if (RELEASE_HEADER.test(line)) {
    if (buffer.length !== 0) {
      releases[`v${getVersionNumber()}`] = buffer.join('\n')
      buffer = []
    }

    buffer.push(line)
    continue
  }

  buffer.push(line)
}

if (buffer.length !== 0) {
  releases[`v${getVersionNumber()}`] = buffer.join('\n')
}

const getTag = async () =>
  new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: 'https:',
        host: 'api.github.com',
        path: `/repos/tulios/kafkajs/tags`,
        headers: {
          'User-Agent': 'KafkaJS Azure Pipeline',
          Accept: 'application/vnd.github.v3+json',
          Authorization: `token ${TOKEN}`,
        },
      },
      res => {
        let rawData = ''

        res.setEncoding('utf8')
        res.on('data', chunk => (rawData += chunk))
        res.on('end', () => {
          try {
            if (res.statusCode !== 200) {
              return reject(new Error(`Error getting tag: ${res.statusCode} - ${rawData}`))
            }

            const data = JSON.parse(rawData)
            const tag = data.find(({ name }) => name === TAG)

            resolve(tag)
          } catch (e) {
            reject(e)
          }
        })
      }
    )

    request.on('error', reject)
    request.end()
  })

const createRelease = async tag =>
  new Promise((resolve, reject) => {
    const request = https.request(
      {
        method: 'post',
        protocol: 'https:',
        host: 'api.github.com',
        path: `/repos/tulios/kafkajs/releases`,
        headers: {
          'User-Agent': 'KafkaJS Azure Pipeline',
          'Content-Type': 'application/json',
          Accept: 'application/vnd.github.v3+json',
          Authorization: `token ${TOKEN}`,
        },
      },
      res => {
        let rawData = ''

        res.setEncoding('utf8')
        res.on('data', chunk => (rawData += chunk))
        res.on('end', () => {
          try {
            if (res.statusCode !== 201) {
              return reject(new Error(`Error creating release: ${res.statusCode}`))
            }

            const data = JSON.parse(rawData)
            resolve(data)
          } catch (e) {
            reject(e)
          }
        })
      }
    )

    request.on('error', reject)
    request.write(
      /* eslint-disable */
      JSON.stringify({
        tag_name: TAG,
        target_commitish: tag.commit.sha,
        name: TAG,
        body: releases[TAG],
        draft: false,
        prerelease: false,
      })
    )
    request.end()
  })

getTag()
  .then(tag => createRelease(tag))
  .then(release => console.log(`Release ${TAG} created`, release))
  .catch(console.error)
