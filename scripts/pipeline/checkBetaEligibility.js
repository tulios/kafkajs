#!/usr/bin/env node

const execa = require('execa')
const { SKIP_BETA_ELIGIBILITY } = process.env

const changedFiles = execa
  .commandSync('git diff --name-only HEAD^1...HEAD', { shell: true })
  .stdout.toString('utf-8')
  .trim()
  .split('\n')

console.log('Env:')
for (const env of Object.keys(process.env)) {
  console.log(`${env}=${process.env[env]}`)
}

console.log(`SKIP_BETA_ELIGIBILITY: "${SKIP_BETA_ELIGIBILITY}"`)

const eligible = filePath =>
  ['index.js', 'package.json'].includes(filePath) || /^(src|types)\//.test(filePath)

if (SKIP_BETA_ELIGIBILITY) {
  console.log('Skip pre-release, SKIP_BETA_ELIGIBILITY flag present')
  process.exit(0)
}

console.log('Changes:')
const hasEligibleFiles = changedFiles.some(filePath => {
  const isEligigle = eligible(filePath)
  console.log(`"${filePath}" - eligigle: ${isEligigle}`)
  return isEligigle
})

const isBumpVersionCommit = changedFiles.every(filePath =>
  ['package.json', 'CHANGELOG.md'].includes(filePath)
)

if (hasEligibleFiles && !isBumpVersionCommit) {
  console.log('Build has eligible files, continue to pre-release')
} else {
  console.log('Skip pre-release, no changes in relevant files')
  console.log(`isBumpVersionCommit: ${isBumpVersionCommit}`)
  console.log('##vso[task.setvariable variable=SKIP_PRE_RELEASE;isOutput=true]true')
}
