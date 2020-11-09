/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react')

const CompLibrary = require('../../core/CompLibrary.js')

const MarkdownBlock = CompLibrary.MarkdownBlock /* Used to read markdown */
const Container = CompLibrary.Container
const GridBlock = CompLibrary.GridBlock

const Button = props => (
  <div className="pluginWrapper buttonWrapper">
    <a className="button" href={props.href} target={props.target}>
      {props.children}
    </a>
  </div>
)

const createLinkGenerator = ({ siteConfig, language = '' }) => {
  const { baseUrl, docsUrl } = siteConfig
  const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`
  const langPart = `${language ? `${language}/` : ''}`
  return doc => `${baseUrl}${docsPart}${langPart}${doc}`
}

class HomeSplash extends React.Component {
  render() {
    const { siteConfig } = this.props
    const { baseUrl } = siteConfig
    const docUrl = createLinkGenerator(this.props)

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    )

    const Logo = props => (
      <img src={props.img_src} alt={siteConfig.title} aria-label="kafka.js.org" />
    )

    const ProjectTitle = () => (
      <h2 className="projectTitle">
        <Logo img_src={`${baseUrl}img/kafkajs-logoV2.svg`} />
        <small>{siteConfig.tagline}</small>
      </h2>
    )

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    )

    return (
      <SplashContainer>
        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} />
          <PromoSection>
            <Button href={docUrl('getting-started')}>Documentation</Button>
            <Button href={siteConfig.repoUrl}>Github</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    )
  }
}

class Index extends React.Component {
  render() {
    const { config: siteConfig, language = '' } = this.props
    const docUrl = createLinkGenerator({ siteConfig, language })

    const Block = props => (
      <Container padding={['bottom', 'top']} id={props.id} background={props.background}>
        <GridBlock align="center" contents={props.children} layout={props.layout} />
      </Container>
    )

    const Features = props => (
      <div id="feature">
        <Block layout="fourColumn">
          {[
            {
              title: 'No Dependencies',
              content:
                'Committed to staying lean and dependency free. 100% Javascript, with no native addons required.',
            },
            {
              title: 'Well Tested',
              content:
                'Every commit is tested against a production-like multi-broker Kafka cluster, ensuring that regressions never make it into production.',
            },
            {
              title: 'Battle Hardened',
              content:
                'Dog-fooded by the authors in dozens of high-traffic services with strict uptime requirements.',
            },
          ]}
        </Block>
      </div>
    )

    const SupportSection = props => (
      <Container className="support" id="support" padding={['top', 'bottom']} background="light">
        <h2>Commercial support available!</h2>
        <p>
          Resolve issues faster with support directly from a <b>KafkaJS expert</b>. We can help you
          with architectural consultations, issue triage &amp; bug fixing, developer training and
          custom development.
        </p>

        <form
          action={props.siteConfig.contactFormUrl}
          method="POST"
          acceptCharset="UTF-8"
          className="contact-form"
        >
          <label htmlFor="email" className="email-label">
            Email
          </label>
          <input type="email" name="email" placeholder="foo@example.com" />

          <label htmlFor="message">What can we help you with?</label>
          <textarea name="message" placeholder="I would like help with ..." rows={3}></textarea>

          <input type="hidden" name="_gotcha" />
          <button type="submit" className="button">
            Submit
          </button>
        </form>

        <small>
          No up-front payment required, but a minimum charge at an agreed upon hourly rate may
          apply. Commercial support is subject to availability. For community support, direct your
          questions at the{' '}
          <a href="https://stackoverflow.com/questions/tagged/kafkajs">
            #kafkajs tag on StackOverflow
          </a>{' '}
          or our <a href={props.siteConfig.slackUrl}>Slack community</a>.
        </small>
      </Container>
    )

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
          {siteConfig.contactFormUrl && <SupportSection siteConfig={siteConfig} />}
        </div>
      </div>
    )
  }
}

module.exports = Index
