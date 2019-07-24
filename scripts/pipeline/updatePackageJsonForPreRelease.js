#!/usr/bin/env node

const path = require('path')
const fs = require('fs')
const execa = require('execa')

const { PRE_RELEASE_VERSION } = process.env

if (!PRE_RELEASE_VERSION) {
  throw new Error('Missing PRE_RELEASE_VERSION env variable')
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
console.log('Package.json patched')
