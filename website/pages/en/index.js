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
          <Container className="trademark-notice">
          <small>KAFKA is a registered trademark of The Apache Software Foundation and has been licensed for use by KafkaJS. KafkaJS has no affiliation with and is not endorsed by The Apache Software Foundation.</small>
      </Container>
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

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
        </div>
      </div>
    )
  }
}

module.exports = Index
