#!/usr/bin/env node

const execa = require('execa')

const changedFiles = execa
  .commandSync('git diff --name-only HEAD^1...HEAD', { shell: true })
  .stdout.toString('utf-8')
  .trim()
  .split('\n')

const eligible = filePath =>
  ['index.js', 'package.json'].includes(filePath) || /^(src|types)\//.test(filePath)

if (!changedFiles.some(filePath => eligible(filePath))) {
  console.log('Skip pre-release, no changes in relevant files')
  execa.commandSync('echo "##vso[task.setvariable variable=SKIP_PRE_RELEASE;isOutput=true]true"', {
    shell: true,
  })
}
