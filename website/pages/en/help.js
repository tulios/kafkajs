/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react')

const CompLibrary = require('../../core/CompLibrary.js')

const Container = CompLibrary.Container
const GridBlock = CompLibrary.GridBlock

function Help(props) {
  const { config: siteConfig, language = '' } = props
  const { baseUrl, docsUrl, slackUrl, repoUrl } = siteConfig
  const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`
  const langPart = `${language ? `${language}/` : ''}`
  const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`

  const supportLinks = [
    {
      content: `Learn more using the [documentation on this site.](${docUrl('getting-started')})`,
      title: 'Browse Docs',
    },
    {
      content: `Ask questions about the documentation and project in [our Slack channel](${slackUrl})`,
      title: 'Join the community',
    },
    {
      content: `Believe you have found a bug? Please [open an issue](https://github.com/tulios/kafkajs/issues) describing the issue as clearly as you can.`,
      title: 'Open an issue',
    },
  ]

  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer documentContainer postContainer">
        <div className="post">
          <header className="postHeader">
            <h1>Need help?</h1>
          </header>
          <p>
            This free, open-source project is developed and maintained by a small group of
            volunteers. Please check if your question is answered already on the{' '}
            <a href={docUrl('faq')}>Frequently Asked Questions page</a> or among the{' '}
            <a href={repoUrl + '/issues'}>Github issues</a>.
          </p>
          <p>
            If you're still unable to find a solution to your problem, feel free to check out the
            links below.
          </p>
          <GridBlock contents={supportLinks} layout="threeColumn" />
        </div>
      </Container>
    </div>
  )
}

module.exports = Help
