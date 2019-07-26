#!/usr/bin/env node

const { coerce, prerelease, parse } = require('semver')
const getCurrentVersion = require('./getCurrentNPMVersion')

const sameStableVersion = (stable, beta) => coerce(stable).version === coerce(beta).version

console.log('Env:')
for (const env of Object.keys(process.env)) {
  console.log(`${env}=${process.env[env]}`)
}

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
