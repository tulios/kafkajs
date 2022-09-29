/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react')

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl
    const docsUrl = this.props.config.docsUrl
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`
    const langPart = `${language ? `${language}/` : ''}`
    return `${baseUrl}${docsPart}${langPart}${doc}`
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl
    return baseUrl + (language ? `${language}/` : '') + doc
  }

  render() {
    return (
      <footer className="nav-footer" id="footer">
        <section className="sitemap">
          <a href={this.props.config.baseUrl} className="nav-home">
            {this.props.config.footerIcon && (
              <img
                src={this.props.config.baseUrl + this.props.config.footerIcon}
                alt={this.props.config.title}
                width="52"
                height="84"
              />
            )}
          </a>
          <div>
            <h5>Docs</h5>
            <a href={this.docUrl('getting-started', this.props.language)}>Usage</a>
          </div>
          <div>
            <h5>Community</h5>
            <a href="https://join.slack.com/t/kafkajs/shared_invite/zt-1ezd5395v-SOpTqYoYfRCyPKOkUggK0A">
              Slack
            </a>
          </div>
          <div>
            <h5>More</h5>
            <a href="https://github.com/tulios/kafkajs">GitHub</a>
            <a
              className="github-button"
              href={this.props.config.repoUrl}
              data-icon="octicon-star"
              data-count-href="/tulios/kafkajs/stargazers"
              data-show-count="true"
              data-count-aria-label="# stargazers on GitHub"
              aria-label="Star this project on GitHub"
            >
              Star
            </a>
            <a href="https://badge.fury.io/js/kafkajs">
              <img src="https://badge.fury.io/js/kafkajs.svg" alt="npm version" height="18" />
            </a>
          </div>
        </section>
      </footer>
    )
  }
}

module.exports = Footer
