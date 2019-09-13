#!/usr/bin/env node

const https = require('https')
const execa = require('execa')

console.log('Env:')
for (const env of Object.keys(process.env)) {
  console.log(`${env}=${process.env[env]}`)
}

const { TOKEN, GITHUB_PR_NUMBER, BUILD_SOURCEVERSIONMESSAGE } = process.env

if (!TOKEN) {
  throw new Error('Missing TOKEN env variable')
}

const githubApi = async ({ payload }) =>
  new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: 'https:',
        host: 'api.github.com',
        path: `/graphql`,
        method: 'POST',
        headers: {
          'User-Agent': 'KafkaJS Azure Pipeline',
          'Content-Type': 'application/json',
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
              return reject(new Error(`Error: ${res.statusCode} - ${rawData}`))
            }

            resolve(JSON.parse(rawData))
          } catch (e) {
            reject(e)
          }
        })
      }
    )

    request.on('error', reject)
    request.write(JSON.stringify(payload))
    request.end()
  })

const commentOnPR = async () => {
  const commitMessage =
    BUILD_SOURCEVERSIONMESSAGE ||
    execa
      .commandSync('git log -1 --pretty=%B', { shell: true })
      .stdout.toString('utf-8')
      .trim()

  const PR_NUMBER_REGEXP = /^Merge pull request #([^\s]+)/
  const result = commitMessage.match(PR_NUMBER_REGEXP)

  if (!GITHUB_PR_NUMBER || !result || !result[1]) {
    console.warn('PR number not found!')
    return
  }

  const prNumber = GITHUB_PR_NUMBER ? parseInt(GITHUB_PR_NUMBER, 10) : parseInt(result[1], 10)
  console.log(`PR number: ${prNumber}`)

  const { data, errors: errorsOnGetPrId } = await githubApi({
    payload: {
      query: `
        query GetPRId($prNumber: Int!) {
          repository(owner: "tulios", name: "kafkajs") {
            pullRequest(number: $prNumber) {
              id
            }
          }
        }
      `,
      variables: { prNumber },
    },
  })

  if (errorsOnGetPrId) {
    console.error(errorsOnGetPrId)
    process.exit(1)
  }

  const subjectId = data.repository.pullRequest.id
  console.log(`subjectId: ${subjectId}`)
  const { version } = require('../../package.json')

  const { errors: errorsOnCommentOnPR } = await githubApi({
    payload: {
      query: `
        mutation CommentOnPR($subjectId: ID!) {
          addComment(input: {
            subjectId: $subjectId,
            clientMutationId: "azure_pipelines_pre_release_hook",
            body: "\`${version}\` released :tada:\nhttps://www.npmjs.com/package/kafkajs/v/${version}"
          }) {
            clientMutationId
          }
        }
      `,
      variables: { subjectId },
    },
  })

  if (errorsOnCommentOnPR) {
    console.error(errorsOnCommentOnPR)
    process.exit(1)
  }
}

commentOnPR()
  .then(console.log)
  .catch(console.error)
