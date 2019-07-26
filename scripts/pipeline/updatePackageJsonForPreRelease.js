#!/usr/bin/env node

const path = require('path')
const fs = require('fs')
const execa = require('execa')

console.log('Env:')
for (const env of Object.keys(process.env)) {
  console.log(`${env}=${process.env[env]}`)
}

const PRE_RELEASE_VERSION = process.env.PRE_RELEASE_VERSION || process.env.BASH2_PRE_RELEASE_VERSION

if (!PRE_RELEASE_VERSION) {
  throw new Error('Missing PRE_RELEASE_VERSION env variable')
}

if (!/\d+\.\d+\.\d+-beta\.\d+/.test(PRE_RELEASE_VERSION)) {
  throw new Error(`Invalid PRE_RELEASE_VERSION: ${PRE_RELEASE_VERSION}`)
}

const packageJson = require('../../package.json')
const commitSha = execa
  .commandSync('git rev-parse --verify HEAD', { shell: true })
  .stdout.toString('utf-8')
  .trim()

packageJson.version = PRE_RELEASE_VERSION
packageJson.kafkajs = {
  sha: commitSha,
  compare: `https://github.com/tulios/kafkajs/compare/master...${commitSha}`,
}

console.log(packageJson.kafkajs)
const filePath = path.resolve(__dirname, '../../package.json')
fs.writeFileSync(filePath, JSON.stringify(packageJson, null, 2))
console.log(`Package.json patched with pre-release version ${PRE_RELEASE_VERSION}`)
